import pytest
from shared.consistent_hash import ConsistentHashRing


def test_consistent_hash_ring_node_addition_and_lookup():
    ring = ConsistentHashRing(virtual_nodes_per_node=50)
    ring.add_node("shard_1")
    ring.add_node("shard_2")
    ring.add_node("shard_3")

    assert len(ring.get_all_nodes()) == 3
    
    node = ring.get_node("doc_1001")
    assert node in ["shard_1", "shard_2", "shard_3"]


def test_consistent_hash_replication_nodes():
    ring = ConsistentHashRing(virtual_nodes_per_node=50)
    ring.add_node("shard_1")
    ring.add_node("shard_2")
    ring.add_node("shard_3")

    nodes = ring.get_nodes("doc_1001", replication_factor=2)
    assert len(nodes) == 2
    assert nodes[0] != nodes[1]  # Primary and replica must be distinct physical nodes


def test_consistent_hash_minimal_remapping_on_churn():
    ring = ConsistentHashRing(virtual_nodes_per_node=100)
    ring.add_node("shard_1")
    ring.add_node("shard_2")
    ring.add_node("shard_3")

    sample_keys = [f"document_key_{i}" for i in range(200)]
    initial_mapping = {key: ring.get_node(key) for key in sample_keys}

    # Add a 4th node
    ring.add_node("shard_4")
    new_mapping = {key: ring.get_node(key) for key in sample_keys}

    # Count how many keys shifted position
    shifted_keys = sum(1 for key in sample_keys if initial_mapping[key] != new_mapping[key])
    # With static hash % N, adding a node re-maps ~75% of keys (1 - 1/4).
    # With consistent hashing, only ~25% (1/4) of keys should move to the new node.
    remapped_ratio = shifted_keys / len(sample_keys)
    assert remapped_ratio < 0.45, f"Remapped ratio {remapped_ratio:.2f} is too high for consistent hashing!"


def test_consistent_hash_ring_distribution_balance():
    ring = ConsistentHashRing(virtual_nodes_per_node=150)
    ring.add_node("node_A")
    ring.add_node("node_B")
    ring.add_node("node_C")

    dist = ring.ring_distribution()
    assert len(dist) == 3
    for node, pct in dist.items():
        # Each node should own roughly 33.3% of ring space (+- 10%)
        assert 23.0 <= pct <= 43.0, f"Node {node} ring share {pct}% is unbalanced."
