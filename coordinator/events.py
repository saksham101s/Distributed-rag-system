import asyncio
import itertools
import json
import logging
import time
from collections import deque
from typing import Any, Deque, Dict, Optional, Set

from pydantic import BaseModel

logger = logging.getLogger("ClusterEvents")


class ClusterEvent(BaseModel):
    """A single cluster event pushed to UI subscribers."""
    id: int
    type: str  # cluster_status | node_down | node_up | repair_completed | document_ingested | document_deleted | fault_injected
    timestamp: float
    data: Dict[str, Any]


def sse(event: str, data: Any, event_id: Optional[int] = None) -> str:
    """Formats one Server-Sent Events frame."""
    frame = f"event: {event}\n"
    if event_id is not None:
        frame += f"id: {event_id}\n"
    return frame + f"data: {json.dumps(data, default=str)}\n\n"


class EventBus:
    """In-process fan-out of cluster events to SSE subscribers.

    Each subscriber gets a bounded queue; a slow subscriber loses its oldest events instead of blocking publishers.
    The last `history_size` notable events (everything except periodic status snapshots) are kept for new subscribers.
    """

    def __init__(self, history_size: int = 50, queue_size: int = 200):
        self._ids = itertools.count(1)
        self._subscribers: Set[asyncio.Queue] = set()
        self._queue_size = queue_size
        self.history: Deque[ClusterEvent] = deque(maxlen=history_size)

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._queue_size)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        self._subscribers.discard(queue)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def publish(self, event_type: str, data: Dict[str, Any]) -> ClusterEvent:
        event = ClusterEvent(id=next(self._ids), type=event_type, timestamp=time.time(), data=data)
        if event_type != "cluster_status":
            self.history.append(event)
        for queue in list(self._subscribers):
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(event)
        return event
