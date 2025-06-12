import heapq
from threading import Event
from config import KAFKA_METRICS_TOPIC, KAFKA_SCHEDULER_TASKS_TOPIC, KAFKA_TRAIN_TOPIC
from redis_util import create_redis_client, get_metadata
from sklearn.ensemble import GradientBoostingRegressor
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer, KafkaConsumer
import pandas as pd
import joblib
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
logger.info("🔧 Logger initialized for Scheduler Service")
# from kafka_util import KafkaSingleton

BATCH_SIZE = 10

r = create_redis_client()

BROKERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
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
            self._model = GradientBoostingRegressor(
                n_estimators=1, max_depth=1)
            self._model.fit([[0, 0, 0, 0, 0, 0, 0]], [60])  # dummy fit
            logger.warning("Runtime model cold‑started with default weights")
        self._buffer: List[Dict] = []

    def _features(self, task: dict):
        return [
            hash(task.get("algo", "")) % 1000,  # algo hash
            int(task.get("n_rows", 0)),  # rows
            int(task.get("n_cols", 0)),  # cols
            task.get("mem_percent_avg", 1024),  # memory MB
            task.get("cpu_percent_avg", 1.5),
            # hash(task.get("metric_name", "")) % 1000,
            task.get("metric_value", 0),
            float(task.get("size_mb",0))
        ]

    def predict(self, task: dict) -> float:
        est = float(self._model.predict([self._features(task)])[0])
        # Apply algorithm‑specific multiplier if present
        mult = float(ALGO_WEIGHT.get(str(task.get("algo", "")).lower(), 1.0))
        return max(est * mult, 1.0)

    def observe(self, task: dict, actual_runtime: float):
        self._buffer.append({"x": self._features(task), "y": actual_runtime})

        logger.info(
            f"📦 Buffer size: {len(self._buffer)} | Contents: {self._buffer}")

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
    tasks_queue: List[dict] = []

    def effective_finish_time(self) -> float:
        """Return load adjusted by speed."""
        if self.speed_factor <= 0:
            return float("inf")
        return self.load_seconds / self.speed_factor


# ----------------------------------------------------------------------------
# Scheduler core
# ----------------------------------------------------------------------------

class Scheduler:
    def __init__(self):
        # if not worker_ids:
        #     raise ValueError("Need at least one worker")
        self.next_worker_id = 1
        self.active_worker_ids = set()
        self.task_estimates = {}
        self.workers: dict[str, WorkerState] = {}
        self.predictor = RuntimePredictor()
        self.shutdown_event = Event()

        # Consumer for TASKS topic
        self.consumer = KafkaConsumer(
            KAFKA_SCHEDULER_TASKS_TOPIC,
            bootstrap_servers=[BROKERS],
            group_id="scheduler-group",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: m.decode('utf-8')
        )

        # Consumer for status topic
        self.status_consumer = KafkaConsumer(
            KAFKA_METRICS_TOPIC,
            bootstrap_servers=[BROKERS],
            group_id="scheduler-status-group",
            auto_offset_reset="earliest",
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
    def release_worker_id(self, wid: str):
        self.workers.pop(wid, None)
        logger.info(f"Released worker ID: {wid}")

    def assign_worker_id(self) -> str:
        wid = str(self.next_worker_id)
        self.next_worker_id += 1

        worker = WorkerState(worker_id=wid)
        self.workers[wid] = worker

        logger.info(f"Assigned new worker ID: {wid}")
        return wid

    def _eligible_workers(self, mem_mb: int) -> List[WorkerState]:
        return [
            w for w in self.workers.values()
            if (w.mem_load_mb + mem_mb) <= w.mem_capacity_mb
        ]

    def _select_worker(self, est_runtime: float, mem_mb: int) -> WorkerState:
        eligible = self._eligible_workers(mem_mb)
        if not eligible:
            logger.warning(
                "No workers with enough memory; falling back to min effective load")
            eligible = list(self.workers.values())
        # Choose min of (effective finish time) + est_runtime
        score_map = {}
        for worker in eligible:
            adjusted_runtime = est_runtime / max(worker.speed_factor, 1e-3)
            score = worker.effective_finish_time() + adjusted_runtime
            score_map[worker.worker_id] = {
                "score": score,
                "load": worker.load_seconds,
                "mem_used": worker.mem_load_mb,
                "speed": worker.speed_factor
            }
        best_worker = min(score_map, key=lambda wid: score_map[wid]["score"])
        return self.workers[best_worker], score_map

    # --------------------------------------------------------------
    # Kafka loops
    # --------------------------------------------------------------

    async def run(self):
        loop = asyncio.get_running_loop()
        await asyncio.gather(
            loop.run_in_executor(None, self._ingress_loop),
            loop.run_in_executor(None, self._status_loop),
            self._heartbeat_monitor_loop()
        )

    async def _heartbeat_monitor_loop(self):
        logger.info("📡 Starting heartbeat monitor...")
        while not self.shutdown_event.is_set():
            await asyncio.sleep(15)
            now = datetime.now(timezone.utc)
            dead_workers = []

            for wid, state in list(self.workers.items()):
                delta = (now - state.last_heartbeat).total_seconds()
                if delta > 10:
                    logger.warning(
                        f"💀 Worker {wid} missed heartbeat ({delta:.1f}s ago). Marking as dead.")
                    dead_workers.append(wid)
            for wid in dead_workers:
                state = self.workers.pop(wid)
                self.release_worker_id(wid)

                for task in state.tasks_queue:
                    logger.info(
                        f"♻️ Reassigning task {task.get('subtask_id')} from dead worker {wid}")
                    await self._requeue_task(task)

    async def _requeue_task(self, task: dict):
        try:
            mem_mb = int(task.get("mem_mb", 1024))
            est = self.predictor.predict(task)
            subtask_id = task.get("subtask_id")
            self.task_estimates[subtask_id] = est

            worker, _ = self._select_worker(est, mem_mb)
            worker.load_seconds += est
            worker.mem_load_mb += mem_mb
            worker.tasks_queue.append(task)

            self.producer.send(
                topic=KAFKA_TRAIN_TOPIC,
                key=worker.worker_id,
                value=json.dumps(task)
            )
            logger.info(
                f"✅ Task {subtask_id} reassigned to worker {worker.worker_id}")
        except Exception:
            logger.exception("❌ Failed to reassign task")

    def _ingress_loop(self):
        logger.info("🔁 Starting _ingress_loop ...")
        while not self.shutdown_event.is_set():
            records = self.consumer.poll()
            if records:
                logger.info("Received Kafka messages: %s", records)
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        task = json.loads(msg.value)
                        mem_mb = int(task.get("mem_mb", 1024))
                        est = self.predictor.predict(task)
                        subtask_id = task.get("subtask_id")
                        self.task_estimates[subtask_id] = est
                        worker, score_map = self._select_worker(est, mem_mb)

                        worker.load_seconds += est
                        worker.mem_load_mb += mem_mb
                        worker.tasks_queue.append(task)
                        self.producer.send(
                            topic=KAFKA_TRAIN_TOPIC,
                            key=worker.worker_id,
                            value=json.dumps(task),
                            headers=msg.headers
                        )

                        logger.info("\nScheduling Task: %s",
                                    task.get("subtask_id", "unknown"))
                        logger.info("Predicted Runtime: %.2f sec", est)
                        logger.info("Task Memory: %d MB", mem_mb)

                        for wid, info in score_map.items():
                            logger.info(
                                "Worker %s → Score: %.2f | Load: %.1fs | Mem Used: %dMB | Speed: %.2f",
                                wid,
                                info["score"],
                                info["load"],
                                info["mem_used"],
                                info["speed"]
                            )
                        logger.info("Selected Worker: %s", worker.worker_id)
                        logger.info(
                            "Task sent to topic '%s' with key '%s'", KAFKA_TRAIN_TOPIC, worker.worker_id)
                    except Exception:
                        logger.exception("Failed to schedule task")

    def _status_loop(self):
        while not self.shutdown_event.is_set():
            try:
                records = self.status_consumer.poll()
                if not records:
                    continue

                logger.info("✅ Fetched raw records from metrics:")
                logger.info(records)
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            status = json.loads(msg.value)
                            wid = status.get("worker_id")
                            start = datetime.fromisoformat(
                                status["started_at"].replace("Z", ""))
                            end = datetime.fromisoformat(
                                status["finished_at"].replace("Z", ""))
                            runtime = (end - start).total_seconds()
                            mem_mb = int(status.get("mem_percent_avg", 0)
                                         * WORKER_MEM_MB_DEFAULT / 100)
                            succeeded = status.get("status") == "DONE"

                            if wid not in self.workers or not succeeded:
                                continue

                            w = self.workers[wid]
                            subtask_id = status.get("subtask_id")
                            metadata = get_metadata(subtask_id, r)
                            print(metadata)
                            status.update(metadata)
                            est_runtime = self.task_estimates.get(
                                subtask_id, runtime)
                            w.load_seconds = max(
                                0.0, w.load_seconds - runtime)

                            w.mem_load_mb = max(0, w.mem_load_mb - mem_mb)
                            w.tasks_queue = [task for task in w.tasks_queue if task.get("subtask_id") != subtask_id]
                            # w.last_heartbeat = datetime.now(timezone.utc)
                            # subtask_id = status.get("subtask_id")
                            # est_runtime = self.task_estimates.get(
                            #     subtask_id, runtime)

                            ratio = (est_runtime + 1e-6) / (runtime + 1e-6)
                            w.speed_factor = max(
                                0.2, min(5.0, 0.8 * w.speed_factor + 0.2 * ratio))
                            logger.info("Here")
                            self.task_estimates.pop(subtask_id, None)

                            self.predictor.observe(status, runtime)
                            logger.info("Worker %s updated: Load=%.1fs | Mem=%dMB | Speed=%.2f | Score=%.2f",
                                        wid, w.load_seconds, w.mem_load_mb, w.speed_factor, w.effective_finish_time())

                        except Exception:
                            logger.exception("Error processing status message")
            except Exception:
                logger.exception("Error in status loop poll")

    # def check_alive_workers(self):
    #     active_ids = list(r.smembers("active_worker_ids"))
    #     alive = []
    #     dead = []

    #     for wid_bytes in active_ids:
    #         wid = wid_bytes.decode() if isinstance(wid_bytes, bytes) else wid_bytes
    #         if r.exists(f"worker_claims:{wid}"):
    #             alive.append(wid)
    #         else:
    #             dead.append(wid)
    #             r.srem("active_worker_ids", wid)
    #             # Reclaim with original order score in sorted set
    #             score = self.worker_id_map.get(
    #                 wid, 10000)  # large default if missing
    #             r.zadd("available_worker_ids", {wid: score})
    #             logger.info(f"Worker {wid} is dead. Reclaiming...")

    #     self.active_worker_ids = alive
    #     logger.info(f"Alive workers: {alive}, Total : {len(alive)}")

    #     return alive, dead


def create_worker_id_pool():
    worker_ids = [f"{i}" for i in range(1, 101)]
    # r.delete("available_worker_ids")
    # r.delete("active_worker_ids")
    # for i, wid in enumerate(worker_ids):
    #     r.zadd("available_worker_ids", {wid: i})  # score = i
    # logger.info(f"Initialized {NUM_WORKERS} worker IDs in Redis.")
    logger.info(f'Created Worker IDs : {worker_ids}')
    return worker_ids


# def assign_worker_id(self) -> str:
#     if not self.available_worker_ids:
#         raise RuntimeError("No free worker IDs available")

#     new_id = self.available_worker_ids.pop()  # Get and remove from available
#     self.active_worker_ids.add(new_id)        # Mark as active
#     return new_id


# def release_worker_id(self, worker_id: str):
#     if worker_id in self.active_worker_ids:
#         self.active_worker_ids.remove(worker_id)
#         self.available_worker_ids.add(worker_id)

# def monitor_workers(scheduler, CHECK_INTERVAL):
#     while True:
#         scheduler.check_alive_workers()
#         time.sleep(CHECK_INTERVAL)
