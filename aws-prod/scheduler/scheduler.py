

"""
Intelligent Task Scheduler for distributed‑machine‑learning
==========================================================
Located in **aws‑prod/master** so the *master node* owns dispatching.
This **v2** adds three finer‑grained signals to improve placement quality:

* **Memory usage** Each job message may carry `mem_mb` (peak memory expected).
  Scheduler tracks per‑worker memory load vs capacity and never over‑commits.
* **Algorithm‑type weighting** Certain algorithms (e.g. GPU‑friendly XGBoost)
  are up‑weighted when estimating runtime so they preferentially target faster
  nodes.  Mapping is configurable via `ALGO_WEIGHT_JSON` env variable.
* **Recent worker performance** The scheduler learns a rolling *speed factor*
  for every worker (EMA of *predicted ÷ actual* runtime).  Jobs are assigned
  to the worker whose **effective finish time** (`(load / speed) + est`) is
  minimal.

Key Kafka topics remain unchanged:
    ingress  → **train‑tasks**
    egress   → **train‑assignments** (key = `worker_id`)
    feedback → **train‑status**

Additional environment variables:
    * WORKER_MEM_MB   – default per‑worker memory capacity (MB), e.g. 16000
    * ALGO_WEIGHT_JSON – JSON dict {"xgboost":1.3,"svm":1.1}

Dependencies (append to `requirements.txt` if missing):
    fastapi~=0.110  uvicorn[standard]  confluent‑kafka~=2.4
    scikit‑learn~=1.5  pandas~=2.2  joblib~=1.4
"""
import asyncio
import json
import threading
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
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from sklearn.ensemble import GradientBoostingRegressor
from scheduler_service import Scheduler, create_worker_id_pool, WorkerState

from redis_util import create_redis_client


NUM_WORKERS = 4
# CHECK_INTERVAL = 2  # seconds


BROKERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INGRESS_TOPIC = os.getenv("SCHED_INGRESS_TOPIC", "tasks")
EGRESS_TOPIC = os.getenv("SCHED_EGRESS_TOPIC", "train")
STATUS_TOPIC = os.getenv("WORKER_STATUS_TOPIC", "metrics")
MODEL_PATH = Path(os.getenv("RUNTIME_MODEL_PATH", "runtime_model.joblib"))

WORKER_MEM_MB_DEFAULT = int(os.getenv("WORKER_MEM_MB", "16000"))


app = FastAPI(title="Distributed‑ML Scheduler", version="2.0")
_scheduler: Optional[Scheduler] = None


class WorkerPatch(BaseModel):
    mem_capacity_mb: Optional[int] = None


class WorkerRegistrationRequest(BaseModel):
    mem_capacity_mb: Optional[int] = WORKER_MEM_MB_DEFAULT
    host: Optional[str] = None


class UnsubscribeRequest(BaseModel):
    worker_id: str


@app.on_event("startup")
async def _startup():
    global _scheduler
    _scheduler = Scheduler()
    asyncio.create_task(_scheduler.run())
    # threading.Thread(target=monitor_workers, args=(
    #     _scheduler, CHECK_INTERVAL), daemon=True).start()
    logger.info("Scheduler started with workers..... ")


@app.get("/workers")
async def workers():
    return list(_scheduler.workers.values())  # type: ignore


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/subscribe")
async def subscribe_worker(req: WorkerRegistrationRequest):
    try:
        wid = _scheduler.assign_worker_id()
        worker = WorkerState(
            worker_id=wid,
            mem_capacity_mb=req.mem_capacity_mb or WORKER_MEM_MB_DEFAULT,
        )
        _scheduler.workers[wid] = worker
        logger.info(f"Worker {wid} registered at {req.host}")
        return {"status": "registered", "worker_id": wid}
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/unsubscribe")
async def unsubscribe_worker(req: UnsubscribeRequest):
    wid = req.worker_id

    if wid not in _scheduler.workers:
        raise HTTPException(status_code=404, detail=f"Worker {wid} not found")
    # Remove worker from active state
    # _scheduler.release_worker_id(wid)

    worker_state = _scheduler.workers.pop(wid, None)
    _scheduler.release_worker_id(wid)

    if worker_state:
        for task in worker_state.tasks_queue:
            logger.info(
                f"♻️ Reassigning task {task.get('subtask_id')} from unsubscribed worker {wid}")
            await _scheduler._requeue_task(task)
    logger.info(
        f"Worker {wid} unsubscribed leaving pool. {wid} now free for assignment")
    return {"status": "unsubscribed", "worker_id": wid}


@app.post("/heartbeat")
async def receive_heartbeat(req: Request):
    data = await req.json()
    wid = data.get("worker_id")
    if not wid or wid not in _scheduler.workers:
        raise HTTPException(status_code=404, detail="Unknown worker ID")

    _scheduler.workers[wid].last_heartbeat = datetime.now(timezone.utc)
    logger.info(f"✅ Received heartbeat from worker {wid}")
    return {"status": "ok"}


@app.get("/queues")
async def task_queues():
    return {
        wid: [task.get("subtask_id") for task in w.tasks_queue]
        for wid, w in _scheduler.workers.items()
    }


def _shutdown(*_):
    logger.info("Gracefully Shutting down scheduler ")
    if _scheduler:
        _scheduler.shutdown_event.set()  # stop all loops
        try:
            _scheduler.consumer.close()
            _scheduler.status_consumer.close()
            _scheduler.producer.flush()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    sys.exit(0)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)
