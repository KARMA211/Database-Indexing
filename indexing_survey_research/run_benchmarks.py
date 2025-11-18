import time
import random
import bisect
import matplotlib.pyplot as plt
import numpy as np

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
        self.num_read_ios += 1
        return node
        
    def _write_node(self, node):
        self.num_write_ios += 1
        return node
        
    def search(self, key):
        self._reset_counters()
        node = self.root
        
        while not node.is_leaf:
            self._read_node(node)
            idx = 0
            while idx < len(node.keys) and key >= node.keys[idx]:
                idx += 1
            node = node.pointers[idx]
            
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
        
        self._insert_into_parent(leaf, new_leaf.keys[0], new_leaf)
        
    def _insert_into_parent(self, left, key, right):
        if left.parent is None:
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

class LSMTree:
    def __init__(self, memtable_size_threshold=100):
        self.memtable = {}
        self.sstables = []
        self.memtable_size_threshold = memtable_size_threshold
        self.num_sequential_writes = 0
        self.num_random_reads = 0
        
    def _reset_counters(self):
        self.num_sequential_writes = 0
        self.num_random_reads = 0
        
    def insert(self, key, value):
        self.memtable[key] = value
        
        if len(self.memtable) >= self.memtable_size_threshold:
            self._flush_memtable()
            
    def _flush_memtable(self):
        if not self.memtable:
            return
            
        sorted_entries = sorted(self.memtable.items())
        self.sstables.append(sorted_entries)
        self.num_sequential_writes += len(sorted_entries)
        self.memtable = {}
        
        if len(self.sstables) > 3:
            self._compact()
            
    def _compact(self):
        if len(self.sstables) < 2:
            return
            
        merged = self._merge_sstables(self.sstables[0], self.sstables[1])
        self.sstables = self.sstables[2:] + [merged]
        self.num_sequential_writes += len(merged)
        
    def _merge_sstables(self, sstable1, sstable2):
        merged = []
        i, j = 0, 0
        
        while i < len(sstable1) and j < len(sstable2):
            if sstable1[i][0] < sstable2[j][0]:
                merged.append(sstable1[i])
                i += 1
            elif sstable1[i][0] > sstable2[j][0]:
                merged.append(sstable2[j])
                j += 1
            else:
                merged.append(sstable2[j])
                i += 1
                j += 1
                
        merged.extend(sstable1[i:])
        merged.extend(sstable2[j:])
        return merged
        
    def search(self, key):
        self._reset_counters()
        
        if key in self.memtable:
            return self.memtable[key], self.num_random_reads
            
        for sstable in reversed(self.sstables):
            self.num_random_reads += 1
            idx = bisect.bisect_left(sstable, (key,))
            if idx < len(sstable) and sstable[idx][0] == key:
                return sstable[idx][1], self.num_random_reads
                
        return None, self.num_random_reads
        
    def range_query(self, low, high):
        self._reset_counters()
        results = []
        
        for key, value in self.memtable.items():
            if low <= key <= high:
                results.append((key, value))
                
        for sstable in self.sstables:
            self.num_random_reads += 1
            start_idx = bisect.bisect_left(sstable, (low,))
            for i in range(start_idx, len(sstable)):
                key, value = sstable[i]
                if key <= high:
                    results.append((key, value))
                else:
                    break
                    
        results.sort(key=lambda x: x[0])
        return results, self.num_random_reads
        
    def force_flush(self):
        self._flush_memtable()

def generate_workload_sequential(size):
    return [(i, f"value_{i}") for i in range(size)]

def generate_workload_random(size):
    keys = list(range(size))
    random.shuffle(keys)
    return [(key, f"value_{key}") for key in keys]

def benchmark_write_amplification():
    print("1. Benchmarking Write Amplification...")
    
    data_sizes = [1000, 2000, 5000, 8000]
    b_tree_waf = []
    lsm_tree_waf = []
    
    for size in data_sizes:
        workload = generate_workload_random(size)
        
        # B+Tree
        b_tree = BPlusTree(order=50)
        total_writes = 0
        for key, value in workload:
            writes = b_tree.insert(key, value)
            total_writes += writes
        b_tree_waf.append(total_writes / size)
        
        # LSM-Tree
        lsm_tree = LSMTree(memtable_size_threshold=100)
        for key, value in workload:
            lsm_tree.insert(key, value)
        lsm_tree.force_flush()
        lsm_tree_waf.append(lsm_tree.num_sequential_writes / size)
    
    # Plot
    plt.figure(figsize=(10, 6))
    plt.plot(data_sizes, b_tree_waf, label='B+Tree', marker='o', linewidth=2)
    plt.plot(data_sizes, lsm_tree_waf, label='LSM-Tree', marker='s', linewidth=2)
    plt.xlabel('Dataset Size')
    plt.ylabel('Write Amplification Factor')
    plt.title('Write Amplification: B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('write_amplification.png', dpi=300, bbox_inches='tight')
    plt.show()

def benchmark_insert_throughput():
    print("2. Benchmarking Insert Throughput...")
    
    data_size = 5000
    workload = generate_workload_random(data_size)
    
    # B+Tree
    b_tree = BPlusTree(order=50)
    b_tree_times = []
    b_tree_counts = []
    
    start_time = time.time()
    for i, (key, value) in enumerate(workload):
        b_tree.insert(key, value)
        if i % 500 == 0:
            b_tree_times.append(time.time() - start_time)
            b_tree_counts.append(i)
    
    # LSM-Tree
    lsm_tree = LSMTree(memtable_size_threshold=500)
    lsm_tree_times = []
    lsm_tree_counts = []
    
    start_time = time.time()
    for i, (key, value) in enumerate(workload):
        lsm_tree.insert(key, value)
        if i % 500 == 0:
            lsm_tree_times.append(time.time() - start_time)
            lsm_tree_counts.append(i)
    
    # Plot
    plt.figure(figsize=(10, 6))
    plt.plot(b_tree_counts, b_tree_times, label='B+Tree', marker='o', linewidth=2)
    plt.plot(lsm_tree_counts, lsm_tree_times, label='LSM-Tree', marker='s', linewidth=2)
    plt.xlabel('Number of Inserts')
    plt.ylabel('Time (seconds)')
    plt.title('Insert Throughput: B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('insert_throughput.png', dpi=300, bbox_inches='tight')
    plt.show()

def benchmark_read_latency():
    print("3. Benchmarking Read Latency...")
    
    data_size = 3000
    workload = generate_workload_random(data_size)
    
    # Build trees
    b_tree = BPlusTree(order=50)
    lsm_tree = LSMTree(memtable_size_threshold=300)
    
    for key, value in workload:
        b_tree.insert(key, value)
        lsm_tree.insert(key, value)
    lsm_tree.force_flush()
    
    # Test reads
    test_keys = random.sample(range(data_size), 50)
    
    b_tree_reads = []
    lsm_tree_reads = []
    
    for key in test_keys:
        _, b_io = b_tree.search(key)
        _, lsm_io = lsm_tree.search(key)
        b_tree_reads.append(b_io)
        lsm_tree_reads.append(lsm_io)
    
    # Plot
    plt.figure(figsize=(10, 6))
    x_pos = np.arange(len(test_keys))
    
    plt.bar(x_pos - 0.2, b_tree_reads, width=0.4, label='B+Tree', alpha=0.7)
    plt.bar(x_pos + 0.2, lsm_tree_reads, width=0.4, label='LSM-Tree', alpha=0.7)
    plt.xlabel('Query Number')
    plt.ylabel('I/O Operations')
    plt.title('Read Latency (I/O Count): B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('read_latency.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print(f"   B+Tree Average I/Os per read: {np.mean(b_tree_reads):.2f}")
    print(f"   LSM-Tree Average I/Os per read: {np.mean(lsm_tree_reads):.2f}")

def main():
    print("Database Indexing Research - Benchmark Suite")
    print("=" * 60)
    
    # Run all benchmarks
    benchmark_write_amplification()
    benchmark_insert_throughput()
    benchmark_read_latency()
    
    print("=" * 60)
    print("All benchmarks completed!")
    print("Graphs saved as: write_amplification.png, insert_throughput.png, read_latency.png")

if __name__ == "__main__":
    main()
