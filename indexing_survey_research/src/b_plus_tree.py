import math

class BPlusTreeNode:
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.pointers = []
        self.next = None
        self.parent = None

class BPlusTree:
    def __init__(self, order=4):
        self.root = BPlusTreeNode(is_leaf=True)
        self.order = order
        self.num_read_ios = 0
        self.num_write_ios = 0
        
    def _reset_counters(self):
        self.num_read_ios = 0
        self.num_write_ios = 0
        
    def _read_node(self, node):
        """Simulate reading a node from disk"""
        self.num_read_ios += 1
        return node
        
    def _write_node(self, node):
        """Simulate writing a node to disk"""
        self.num_write_ios += 1
        return node
        
    def search(self, key):
        self._reset_counters()
        node = self.root
        
        while not node.is_leaf:
            self._read_node(node)
            # Find the appropriate child
            idx = 0
            while idx < len(node.keys) and key >= node.keys[idx]:
                idx += 1
            node = node.pointers[idx]
            
        # Search in leaf node
        self._read_node(node)
        for i, k in enumerate(node.keys):
            if k == key:
                return node.pointers[i], self.num_read_ios
        return None, self.num_read_ios
        
    def insert(self, key, value):
        self._reset_counters()
        leaf = self._find_leaf(key)
        self._insert_into_leaf(leaf, key, value)
        return self.num_write_ios
        
    def _find_leaf(self, key):
        node = self.root
        while not node.is_leaf:
            self._read_node(node)
            idx = 0
            while idx < len(node.keys) and key >= node.keys[idx]:
                idx += 1
            node = node.pointers[idx]
        return node
        
    def _insert_into_leaf(self, leaf, key, value):
        # Find position to insert
        pos = 0
        while pos < len(leaf.keys) and leaf.keys[pos] < key:
            pos += 1
            
        leaf.keys.insert(pos, key)
        leaf.pointers.insert(pos, value)
        self._write_node(leaf)
        
        if len(leaf.keys) > self.order - 1:
            self._split_leaf(leaf)
            
    def _split_leaf(self, leaf):
        mid = len(leaf.keys) // 2
        new_leaf = BPlusTreeNode(is_leaf=True)
        
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.pointers = leaf.pointers[mid:]
        leaf.keys = leaf.keys[:mid]
        leaf.pointers = leaf.pointers[:mid]
        
        new_leaf.next = leaf.next
        leaf.next = new_leaf
        new_leaf.parent = leaf.parent
        
        self._write_node(new_leaf)
        self._write_node(leaf)
        
        # Insert the middle key into parent
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)
        
    def _insert_into_parent(self, left, key, right):
        if left.parent is None:
            # Create new root
            new_root = BPlusTreeNode()
            new_root.keys = [key]
            new_root.pointers = [left, right]
            left.parent = new_root
            right.parent = new_root
            self.root = new_root
            self._write_node(new_root)
            return
            
        parent = left.parent
        self._read_node(parent)
        
        # Find position to insert
        pos = 0
        while pos < len(parent.keys) and parent.keys[pos] < key:
            pos += 1
            
        parent.keys.insert(pos, key)
        parent.pointers.insert(pos + 1, right)
        self._write_node(parent)
        
        if len(parent.keys) > self.order - 1:
            self._split_internal(parent)
            
    def _split_internal(self, node):
        mid = len(node.keys) // 2
        split_key = node.keys[mid]
        
        new_node = BPlusTreeNode()
        new_node.keys = node.keys[mid+1:]
        new_node.pointers = node.pointers[mid+1:]
        node.keys = node.keys[:mid]
        node.pointers = node.pointers[:mid+1]
        
        # Update parent pointers for children
        for pointer in new_node.pointers:
            pointer.parent = new_node
            
        new_node.parent = node.parent
        self._write_node(new_node)
        self._write_node(node)
        
        if node.parent is not None:
            self._insert_into_parent(node, split_key, new_node)
            
    def range_query(self, low, high):
        self._reset_counters()
        start_node = self._find_leaf(low)
        results = []
        current = start_node
        
        while current:
            self._read_node(current)
            for i, key in enumerate(current.keys):
                if low <= key <= high:
                    results.append((key, current.pointers[i]))
                elif key > high:
                    return results, self.num_read_ios
            current = current.next
            
        return results, self.num_read_ios
