"""
Celery application for the AP Executor.

This is the entrypoint for a new worker process that runs AP execution tasks.
A standalone celery worker can be run with:

    uv run celery -A ap_executor.celery_app:celery_app worker --loglevel=info
"""
from celery import Celery
from dotenv import load_dotenv

from ap_executor.di import REDIS_BROKER_URI, REDIS_ENABLED

load_dotenv()

# Celery is only fully configured when a Redis broker URI is available.
# When Redis is absent the app object still exists (imports won't break)
# but no tasks can be dispatched.
_broker = REDIS_BROKER_URI or "memory://"

celery_app = Celery(
    "ap_executor",
    broker=_broker,
    backend=_broker,
    include=["ap_executor.tasks.execute"] if REDIS_ENABLED else [],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    result_expires=3600,  # Results are kept for 1 hour
)
