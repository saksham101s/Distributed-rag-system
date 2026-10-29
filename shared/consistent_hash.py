import hashlib
import bisect
import logging
from typing import List, Set, Dict, Optional, Tuple

logger = logging.getLogger("ConsistentHashRing")


class ConsistentHashRing:
    """Hand-rolled Consistent Hash Ring with Virtual Nodes.

    Maps document keys to physical shard nodes across a 32-bit integer ring space [0, 2^32 - 1].
    Virtual nodes ensure uniform key distribution across physical nodes and minimize key migration
    when nodes join or leave the cluster.
    """

    def __init__(self, virtual_nodes_per_node: int = 150):
        self.virtual_nodes_per_node = virtual_nodes_per_node
        self.ring: List[Tuple[int, str]] = []  # Sorted list of (hash_value, physical_node_id)
        self._hashes: List[int] = []           # Parallel list of hash values, kept for bisect
        self.physical_nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """Computes 32-bit integer hash for string key using MD5."""
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        # Take first 8 hex characters -> 32-bit unsigned int
        return int(digest[:8], 16)

    def _rebuild_hashes(self):
        self._hashes = [h_val for h_val, _ in self.ring]

    def add_node(self, node_id: str) -> bool:
        """Adds a physical node to the ring by creating virtual node entries."""
        if node_id in self.physical_nodes:
            logger.warning(f"Node '{node_id}' already exists in ring.")
            return False

        self.physical_nodes.add(node_id)
        self.ring.extend((self._hash(f"{node_id}#vnode_{i}"), node_id) for i in range(self.virtual_nodes_per_node))
        self.ring.sort()
        self._rebuild_hashes()

        logger.info(f"Added physical node '{node_id}' ({self.virtual_nodes_per_node} vnodes). Total ring size: {len(self.ring)}")
        return True

    def remove_node(self, node_id: str) -> bool:
        """Removes a physical node and all its virtual nodes from the ring."""
        if node_id not in self.physical_nodes:
            logger.warning(f"Node '{node_id}' not found in ring.")
            return False

        self.physical_nodes.remove(node_id)
        self.ring = [entry for entry in self.ring if entry[1] != node_id]
        self._rebuild_hashes()
        logger.info(f"Removed physical node '{node_id}'. Total ring size: {len(self.ring)}")
        return True

    def get_node(self, key: str) -> Optional[str]:
        """Returns the primary physical node responsible for a given document key."""
        nodes = self.get_nodes(key, replication_factor=1)
        return nodes[0] if nodes else None

    def get_nodes(self, key: str, replication_factor: int = 2) -> List[str]:
        """Returns ordered list of primary + distinct replica physical nodes for a given key."""
        if not self.ring:
            return []

        # First virtual node clockwise from the key's hash (binary search on the cached hash list)
        idx = bisect.bisect_right(self._hashes, self._hash(key)) % len(self.ring)
        wanted = min(replication_factor, len(self.physical_nodes))

        selected_nodes: List[str] = []
        for step in range(len(self.ring)):
            if len(selected_nodes) >= wanted:
                break
            _, physical_id = self.ring[(idx + step) % len(self.ring)]
            if physical_id not in selected_nodes:
                selected_nodes.append(physical_id)

        return selected_nodes

    def get_all_nodes(self) -> List[str]:
        """Returns list of all active physical nodes."""
        return sorted(self.physical_nodes)

    def ring_distribution(self) -> Dict[str, float]:
        """Calculates percentage share of the 32-bit key space owned (as primary) by each physical node."""
        if not self.ring:
            return {}

        space = 2 ** 32
        owned: Dict[str, int] = {node_id: 0 for node_id in self.physical_nodes}
        # Each vnode owns the arc from the previous vnode (exclusive) up to itself (inclusive)
        for i, (h_val, node_id) in enumerate(self.ring):
            prev = self.ring[i - 1][0] if i > 0 else self.ring[-1][0] - space
            owned[node_id] += h_val - prev

        return {node_id: round(arc / space * 100, 2) for node_id, arc in owned.items()}
