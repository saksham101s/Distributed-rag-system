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
        self.physical_nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """Computes 32-bit integer hash for string key using MD5."""
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        # Take first 8 hex characters -> 32-bit unsigned int
        return int(digest[:8], 16)

    def add_node(self, node_id: str) -> bool:
        """Adds a physical node to the ring by creating virtual node entries."""
        if node_id in self.physical_nodes:
            logger.warning(f"Node '{node_id}' already exists in ring.")
            return False

        self.physical_nodes.add(node_id)
        for i in range(self.virtual_nodes_per_node):
            vnode_key = f"{node_id}#vnode_{i}"
            h_val = self._hash(vnode_key)
            bisect.insort(self.ring, (h_val, node_id))

        logger.info(f"Added physical node '{node_id}' ({self.virtual_nodes_per_node} vnodes). Total ring size: {len(self.ring)}")
        return True

    def remove_node(self, node_id: str) -> bool:
        """Removes a physical node and all its virtual nodes from the ring."""
        if node_id not in self.physical_nodes:
            logger.warning(f"Node '{node_id}' not found in ring.")
            return False

        self.physical_nodes.remove(node_id)
        self.ring = [(h_val, physical_id) for h_val, physical_id in self.ring if physical_id != node_id]
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

        h_val = self._hash(key)
        # Find index of first virtual node with hash >= h_val using binary search
        ring_hashes = [entry[0] for entry in self.ring]
        idx = bisect.bisect_right(ring_hashes, h_val) % len(self.ring)

        selected_nodes: List[str] = []
        visited_indices: Set[int] = set()
        
        # Traverse ring clockwise to collect 'replication_factor' unique physical nodes
        curr_idx = idx
        while len(selected_nodes) < min(replication_factor, len(self.physical_nodes)):
            if curr_idx in visited_indices:
                break
            visited_indices.add(curr_idx)

            _, physical_id = self.ring[curr_idx]
            if physical_id not in selected_nodes:
                selected_nodes.append(physical_id)

            curr_idx = (curr_idx + 1) % len(self.ring)

        return selected_nodes

    def get_all_nodes(self) -> List[str]:
        """Returns list of all active physical nodes."""
        return sorted(list(self.physical_nodes))

    def ring_distribution(self) -> Dict[str, float]:
        """Calculates percentage share of ring space owned by each physical node."""
        if not self.ring:
            return {}

        node_counts: Dict[str, int] = {node_id: 0 for node_id in self.physical_nodes}
        total_slots = len(self.ring)
        
        for _, node_id in self.ring:
            node_counts[node_id] += 1

        return {node_id: round((count / total_slots) * 100, 2) for node_id, count in node_counts.items()}
