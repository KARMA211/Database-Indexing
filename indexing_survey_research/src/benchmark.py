import time
import random
import matplotlib.pyplot as plt
import numpy as np
from b_plus_tree import BPlusTree
from lsm_tree import LSMTree

def generate_workload_sequential(size):
    """Generate sequential keys"""
    return [(i, f"value_{i}") for i in range(size)]

def generate_workload_random(size):
    """Generate random keys"""
    keys = list(range(size))
    random.shuffle(keys)
    return [(key, f"value_{key}") for key in keys]

def benchmark_write_amplification():
    """Measure write amplification for different dataset sizes"""
    print("Running Write Amplification Benchmark...")
    
    data_sizes = [1000, 2000, 5000, 8000, 10000]
    b_tree_waf = []
    lsm_tree_waf = []
    
    for size in data_sizes:
        print(f"Testing size {size}...")
        workload = generate_workload_random(size)
        
        # Test B+Tree
        b_tree = BPlusTree(order=50)
        total_writes = 0
        for key, value in workload:
            writes = b_tree.insert(key, value)
            total_writes += writes
        b_tree_waf.append(total_writes / size)
        
        # Test LSM-Tree
        lsm_tree = LSMTree(memtable_size_threshold=100)
        for key, value in workload:
            lsm_tree.insert(key, value)
        lsm_tree.force_flush()  # Ensure all data is flushed
        lsm_tree_waf.append(lsm_tree.num_sequential_writes / size)
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.plot(data_sizes, b_tree_waf, label='B+Tree', marker='o', linewidth=2)
    plt.plot(data_sizes, lsm_tree_waf, label='LSM-Tree', marker='s', linewidth=2)
    plt.xlabel('Dataset Size')
    plt.ylabel('Write Amplification Factor')
    plt.title('Write Amplification: B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('../results/write_amplification.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return b_tree_waf, lsm_tree_waf

def benchmark_insert_throughput():
    """Measure insert throughput over time"""
    print("Running Insert Throughput Benchmark...")
    
    data_size = 10000
    workload = generate_workload_random(data_size)
    
    # Test B+Tree
    b_tree = BPlusTree(order=50)
    b_tree_times = []
    b_tree_counts = []
    
    start_time = time.time()
    for i, (key, value) in enumerate(workload):
        b_tree.insert(key, value)
        if i % 1000 == 0:  # Measure every 1000 inserts
            b_tree_times.append(time.time() - start_time)
            b_tree_counts.append(i)
    
    # Test LSM-Tree
    lsm_tree = LSMTree(memtable_size_threshold=1000)
    lsm_tree_times = []
    lsm_tree_counts = []
    
    start_time = time.time()
    for i, (key, value) in enumerate(workload):
        lsm_tree.insert(key, value)
        if i % 1000 == 0:
            lsm_tree_times.append(time.time() - start_time)
            lsm_tree_counts.append(i)
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.plot(b_tree_counts, b_tree_times, label='B+Tree', marker='o', linewidth=2)
    plt.plot(lsm_tree_counts, lsm_tree_times, label='LSM-Tree', marker='s', linewidth=2)
    plt.xlabel('Number of Inserts')
    plt.ylabel('Time (seconds)')
    plt.title('Insert Throughput: B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('../results/insert_throughput.png', dpi=300, bbox_inches='tight')
    plt.close()

def benchmark_read_latency():
    """Measure read latency for different operations"""
    print("Running Read Latency Benchmark...")
    
    data_size = 5000
    workload = generate_workload_random(data_size)
    
    # Build trees with data
    b_tree = BPlusTree(order=50)
    lsm_tree = LSMTree(memtable_size_threshold=500)
    
    for key, value in workload:
        b_tree.insert(key, value)
        lsm_tree.insert(key, value)
    lsm_tree.force_flush()
    
    # Test point queries
    test_keys = random.sample(range(data_size), 100)
    
    b_tree_reads = []
    lsm_tree_reads = []
    
    for key in test_keys:
        _, b_io = b_tree.search(key)
        _, lsm_io = lsm_tree.search(key)
        b_tree_reads.append(b_io)
        lsm_tree_reads.append(lsm_io)
    
    # Plot results
    plt.figure(figsize=(10, 6))
    x_pos = np.arange(len(test_keys))
    
    plt.plot(x_pos, b_tree_reads, label='B+Tree I/O Count', alpha=0.7)
    plt.plot(x_pos, lsm_tree_reads, label='LSM-Tree I/O Count', alpha=0.7)
    plt.xlabel('Query Number')
    plt.ylabel('I/O Operations')
    plt.title('Read Latency (I/O Count): B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('../results/read_latency.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    return np.mean(b_tree_reads), np.mean(lsm_tree_reads)

def benchmark_range_queries():
    """Measure range query performance"""
    print("Running Range Query Benchmark...")
    
    data_size = 5000
    workload = generate_workload_sequential(data_size)
    
    # Build trees with data
    b_tree = BPlusTree(order=50)
    lsm_tree = LSMTree(memtable_size_threshold=1000)
    
    for key, value in workload:
        b_tree.insert(key, value)
        lsm_tree.insert(key, value)
    lsm_tree.force_flush()
    
    range_sizes = [10, 50, 100, 200, 500]
    b_tree_ios = []
    lsm_tree_ios = []
    
    for range_size in range_sizes:
        low = random.randint(0, data_size - range_size)
        high = low + range_size
        
        _, b_io = b_tree.range_query(low, high)
        _, lsm_io = lsm_tree.range_query(low, high)
        
        b_tree_ios.append(b_io)
        lsm_tree_ios.append(lsm_io)
    
    # Plot results
    plt.figure(figsize=(10, 6))
    plt.plot(range_sizes, b_tree_ios, label='B+Tree', marker='o', linewidth=2)
    plt.plot(range_sizes, lsm_tree_ios, label='LSM-Tree', marker='s', linewidth=2)
    plt.xlabel('Range Size')
    plt.ylabel('I/O Operations')
    plt.title('Range Query Performance: B+Tree vs LSM-Tree')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig('../results/range_queries.png', dpi=300, bbox_inches='tight')
    plt.close()

if __name__ == "__main__":
    # Run all benchmarks
    benchmark_write_amplification()
    benchmark_insert_throughput()
    benchmark_read_latency()
    benchmark_range_queries()
    print("All benchmarks completed! Check the /results folder for graphs.")
