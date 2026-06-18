"""Drain S3 event notifications (S3 -> SQS) into the existing WatchEvent queue.

This is the cloud-native replacement for the watchdog/inotify observer: it is a
PRODUCER that only enqueues WatchEvents, leaving the single worker as the sole
DB writer. SQS delivery is at-least-once, so an event may arrive twice — which
is safe precisely because the worker is idempotent: re-cataloging an unchanged
file is an ETag-skip and deleting an already-gone row is a no-op.

Like ``s3_storage``, this module does not import boto3; the SQS client is
injected so it stays testable and AWS stays a runtime-only dependency.
"""

import json
import logging
import queue
import threading
import urllib.parse

from .watcher import WatchEvent

logger = logging.getLogger(__name__)


def s3_event_producer(
    sqs,
    queue_url: str,
    work_q: "queue.Queue[WatchEvent]",
    stop_event: threading.Event,
) -> None:
    """Long-poll ``queue_url`` and translate S3 notifications into WatchEvents.

    Runs until ``stop_event`` is set. Each SQS message is acked (deleted) only
    after its records are enqueued, so a crash mid-batch redelivers rather than
    drops — the worker's idempotency absorbs the duplicate.
    """
    while not stop_event.is_set():
        resp = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20,
        )
        for msg in resp.get("Messages", []):
            try:
                _enqueue_records(msg.get("Body", "{}"), work_q)
            except Exception:  # noqa: BLE001 - a malformed body must not kill the loop
                logger.exception("skipping unparseable SQS message")
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])


def _enqueue_records(body: str, work_q: "queue.Queue[WatchEvent]") -> None:
    for rec in json.loads(body).get("Records", []):
        name = rec.get("eventName", "")
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        if not key.endswith(".mcap"):
            continue
        if name.startswith("ObjectCreated"):
            work_q.put(WatchEvent("catalog", key))
        elif name.startswith("ObjectRemoved"):
            work_q.put(WatchEvent("delete", key))
