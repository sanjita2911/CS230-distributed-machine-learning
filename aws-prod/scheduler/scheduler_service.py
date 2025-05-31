import asyncio
import json
import redis
import time
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from logger_util import logger
import joblib
import pandas as pd
# from kafka_util import KafkaSingleton
from kafka import KafkaProducer, KafkaConsumer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sklearn.ensemble import GradientBoostingRegressor
from redis_util import create_redis_client

REDIS_HOST = "localhost"
REDIS_PORT = 6379
NUM_WORKERS = 10
BATCH_SIZE = 128

r = create_redis_client()

BROKERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INGRESS_TOPIC = os.getenv("SCHED_INGRESS_TOPIC", "tasks")
EGRESS_TOPIC = os.getenv("SCHED_EGRESS_TOPIC", "train")
STATUS_TOPIC = os.getenv("WORKER_STATUS_TOPIC", "metrics")
MODEL_PATH = Path(os.getenv("RUNTIME_MODEL_PATH", "runtime_model.joblib"))

WORKER_MEM_MB_DEFAULT = int(os.getenv("WORKER_MEM_MB", "16000"))

# Algorithm‑weight mapping – e.g. {"xgboost":1.3,"randomforest":1.0}
try:
    ALGO_WEIGHT = json.loads(os.getenv("ALGO_WEIGHT_JSON", "{}"))
except json.JSONDecodeError:
    ALGO_WEIGHT = {}

class RuntimePredictor:
    """GBRT model with online refresh."""

    def __init__(self):
        if MODEL_PATH.exists():
            self._model = joblib.load(MODEL_PATH)
            logger.info("Runtime model loaded from %s", MODEL_PATH)
        else:
            self._model = GradientBoostingRegressor(n_estimators=1, max_depth=1)
            self._model.fit([[0, 0, 0, 0, 0]], [60])  # dummy fit
            logger.warning("Runtime model cold‑started with default weights")
        self._buffer: List[Dict] = []

    def _features(self, task: dict):
        return [
            hash(task.get("algo", "")) % 1000,  # algo hash
            task.get("n_rows", 0),  # rows
            task.get("n_cols", 0),  # cols
            task.get("hp_grid_size", 1),  # trials
            task.get("mem_mb", 1024),  # memory MB
        ]

    def predict(self, task: dict) -> float:
        est = float(self._model.predict([self._features(task)])[0])
        # Apply algorithm‑specific multiplier if present
        mult = float(ALGO_WEIGHT.get(str(task.get("algo", "")).lower(), 1.0))
        return max(est * mult, 1.0)

    def observe(self, task: dict, actual_runtime: float):
        self._buffer.append({"x": self._features(task), "y": actual_runtime})
        if len(self._buffer) >= BATCH_SIZE:
            df = pd.DataFrame(self._buffer)
            self._model.fit(list(df["x"].values), list(df["y"].values))
            joblib.dump(self._model, MODEL_PATH)
            logger.info("Runtime model retrained on %d samples", len(df))
            self._buffer.clear()


# ----------------------------------------------------------------------------
# Worker state with memory + speed factor
# ----------------------------------------------------------------------------

class WorkerState(BaseModel):
    worker_id: str
    load_seconds: float = 0.0  # queued runtime in seconds
    mem_load_mb: int = 0  # queued memory usage
    mem_capacity_mb: int = WORKER_MEM_MB_DEFAULT
    speed_factor: float = 1.0  # >1 means faster than baseline
    last_heartbeat: datetime = datetime.now(timezone.utc)

    def effective_finish_time(self) -> float:
        """Return load adjusted by speed."""
        if self.speed_factor <= 0:
            return float("inf")
        return self.load_seconds / self.speed_factor


# ----------------------------------------------------------------------------
# Scheduler core
# ----------------------------------------------------------------------------

class Scheduler:
    def __init__(self, worker_ids: List[str]):
        if not worker_ids:
            raise ValueError("Need at least one worker")
        self.active_worker_ids: List[str] = worker_ids.copy()
        self.worker_id_map = {self.active_worker_ids[i]: i for i in range(NUM_WORKERS)}
        self.workers: Dict[str, WorkerState] = {
            wid: WorkerState(worker_id=wid) for wid in worker_ids
        }
        self.predictor = RuntimePredictor()

        # Consumer for ingress topic
        self.consumer = KafkaConsumer(
            INGRESS_TOPIC,
            bootstrap_servers=[BROKERS],
            group_id="scheduler-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: m.decode('utf-8')
        )

        # Consumer for status topic
        self.status_consumer = KafkaConsumer(
            STATUS_TOPIC,
            bootstrap_servers=[BROKERS],
            group_id="scheduler-status-group",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda m: m.decode('utf-8')
        )

        # Producer
        self.producer = KafkaProducer(
            bootstrap_servers=[BROKERS],
            acks='all',  # for idempotence-like durability
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )

    # --------------------------------------------------------------
    # Worker selection
    # --------------------------------------------------------------

    def _eligible_workers(self, mem_mb: int) -> List[WorkerState]:
        return [
            w for w in self.workers.values()
            if (w.mem_load_mb + mem_mb) <= w.mem_capacity_mb
        ]

    def _select_worker(self, est_runtime: float, mem_mb: int) -> WorkerState:
        eligible = self._eligible_workers(mem_mb)
        if not eligible:
            # Fallback: ignore memory but log warning
            logger.warning("No workers with enough memory; falling back to min effective load")
            eligible = list(self.workers.values())
        # Choose min of (effective finish time) + est_runtime
        return min(eligible, key=lambda w: w.effective_finish_time() + est_runtime)

    # --------------------------------------------------------------
    # Kafka loops
    # --------------------------------------------------------------

    async def run(self):
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            loop.run_in_executor(None, self._ingress_loop),
            loop.run_in_executor(None, self._status_loop)
        )

    def _ingress_loop(self):
        while True:
            records = self.consumer.poll()
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        task = json.loads(msg.value)
                        mem_mb = int(task.get("mem_mb", 1024))
                        est = self.predictor.predict(task)
                        worker = self._select_worker(est, mem_mb)

                        worker.load_seconds += est
                        worker.mem_load_mb += mem_mb

                        self.producer.send(
                            topic=EGRESS_TOPIC,
                            key=worker.worker_id.encode(),
                            value=msg.value,
                            headers=msg.headers
                        )

                        logger.info("Task %s → %s  est=%.1fs mem=%dMB", task.get("task_id"), worker.worker_id, est,
                                    mem_mb)
                    except Exception:
                        logger.exception("Failed to schedule task")

    def _status_loop(self):
        while True:
            try:
                records = self.status_consumer.poll()
                if not records:
                    continue

                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            status = json.loads(msg.value)
                            wid = status.get("worker_id")
                            runtime = float(status.get("runtime", 0))
                            mem_mb = int(status.get("mem_mb", 0))
                            succeeded = status.get("status") == "DONE"
                            est_runtime = float(status.get("est_runtime", runtime))

                            if wid not in self.workers or not succeeded:
                                continue

                            w = self.workers[wid]
                            w.load_seconds = max(0.0, w.load_seconds - runtime)
                            w.mem_load_mb = max(0, w.mem_load_mb - mem_mb)
                            w.last_heartbeat = datetime.now(timezone.utc)

                            ratio = (est_runtime + 1e-6) / (runtime + 1e-6)
                            w.speed_factor = max(0.2, min(5.0, 0.8 * w.speed_factor + 0.2 * ratio))

                            self.predictor.observe(status, runtime)
                            logger.info("Worker %s DONE task %s runtime=%.1fs ratio=%.2f speed=%.2f",
                                        wid, status.get("task_id"), runtime, ratio, w.speed_factor)

                        except Exception:
                            logger.exception("Error processing status message")
            except Exception:
                logger.exception("Error in status loop poll")

    def check_alive_workers(self):
        active_ids = list(r.smembers("active_worker_ids"))
        alive = []
        dead = []

        for wid_bytes in active_ids:
            wid = wid_bytes.decode() if isinstance(wid_bytes, bytes) else wid_bytes
            if r.exists(f"worker_claims:{wid}"):
                alive.append(wid)
            else:
                dead.append(wid)
                r.srem("active_worker_ids", wid)
                # Reclaim with original order score in sorted set
                score = self.worker_id_map.get(wid, 10000)  # large default if missing
                r.zadd("available_worker_ids", {wid: score})
                logger.info(f"Worker {wid} is dead. Reclaiming...")

        self.active_worker_ids = alive
        logger.info(f"Alive workers: {alive}, Total : {len(alive)}")

        return alive, dead

def create_worker_id_pool():
    worker_ids = [f"worker-{i:03d}" for i in range(NUM_WORKERS)]
    r.delete("available_worker_ids")
    r.delete("active_worker_ids")
    for i, wid in enumerate(worker_ids):
        r.zadd("available_worker_ids", {wid: i})  # score = i
    logger.info(f"Initialized {NUM_WORKERS} worker IDs in Redis.")
    return worker_ids

def monitor_workers(scheduler, CHECK_INTERVAL):
    while True:
        scheduler.check_alive_workers()
        time.sleep(CHECK_INTERVAL)
