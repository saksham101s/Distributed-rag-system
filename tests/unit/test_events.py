import asyncio
import json

from coordinator.events import EventBus, sse


async def test_publish_fans_out_and_keeps_history():
    bus = EventBus(history_size=2, queue_size=2)
    q1, q2 = bus.subscribe(), bus.subscribe()
    bus.publish("node_down", {"shard_id": "s1"})
    bus.publish("cluster_status", {"healthy_nodes": 2})  # not kept in history
    bus.publish("node_up", {"shard_id": "s1"})

    # queue_size=2: the oldest event is dropped for slow subscribers
    assert [q1.get_nowait().type for _ in range(2)] == ["cluster_status", "node_up"]
    assert q2.qsize() == 2
    assert [e.type for e in bus.history] == ["node_down", "node_up"]

    bus.unsubscribe(q1)
    bus.publish("node_down", {"shard_id": "s2"})
    assert q1.empty()


def test_sse_frame_format():
    frame = sse("token", {"text": "hi"}, event_id=7)
    assert frame == 'event: token\nid: 7\ndata: {"text": "hi"}\n\n'
    assert json.loads(frame.split("data: ")[1]) == {"text": "hi"}
