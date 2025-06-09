

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
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sklearn.ensemble import GradientBoostingRegressor
from scheduler_service import Scheduler, create_worker_id_pool

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


@app.on_event("startup")
async def _startup():
    global _scheduler
   # _worker_ids = create_worker_id_pool()
    _scheduler = Scheduler()
    asyncio.create_task(_scheduler.run())
    # threading.Thread(target=monitor_workers, args=(
    #     _scheduler, CHECK_INTERVAL), daemon=True).start()
    logger.info("Scheduler started with workers..... ")


class WorkerPatch(BaseModel):
    mem_capacity_mb: Optional[int] = None


@app.patch("/workers/{worker_id}")
async def patch_worker(worker_id: str, patch: WorkerPatch):
    if worker_id not in _scheduler.workers:  # type: ignore
        raise HTTPException(404, "Worker not found")
    w = _scheduler.workers[worker_id]  # type: ignore
    if patch.mem_capacity_mb is not None:
        w.mem_capacity_mb = patch.mem_capacity_mb
    return w


@app.get("/workers")
async def workers():
    return list(_scheduler.workers.values())  # type: ignore


@app.get("/health")
async def health():
    return {"status": "ok"}


def _shutdown(*_):
    logger.info("Shutting down scheduler")
    if _scheduler:
        _scheduler.consumer.close()
        _scheduler.status_consumer.close()
        _scheduler.producer.flush()
    sys.exit(0)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)
