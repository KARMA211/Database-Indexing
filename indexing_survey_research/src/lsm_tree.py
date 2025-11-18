import bisect

class LSMTree:
    def __init__(self, memtable_size_threshold=100):
        self.memtable = {}
        self.sstables = []  # List of sorted key-value pairs
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
            
        # Create sorted SSTable from memtable
        sorted_entries = sorted(self.memtable.items())
        self.sstables.append(sorted_entries)
        
        # Simulate sequential write (size of data written)
        self.num_sequential_writes += len(sorted_entries)
        
        self.memtable = {}
        
        # Simple compaction: merge if too many SSTables
        if len(self.sstables) > 3:
            self._compact()
            
    def _compact(self):
        if len(self.sstables) < 2:
            return
            
        # Merge the two oldest SSTables
        merged = self._merge_sstables(self.sstables[0], self.sstables[1])
        self.sstables = self.sstables[2:] + [merged]
        
        # Count the write of merged data
        self.num_sequential_writes += len(merged)
        
    def _merge_sstables(self, sstable1, sstable2):
        """Merge two sorted SSTables, removing duplicates (newer values win)"""
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
                # Keys are equal, take the newer one (from sstable2)
                merged.append(sstable2[j])
                i += 1
                j += 1
                
        # Add remaining entries
        merged.extend(sstable1[i:])
        merged.extend(sstable2[j:])
        
        return merged
        
    def search(self, key):
        self._reset_counters()
        
        # Check memtable first
        if key in self.memtable:
            return self.memtable[key], self.num_random_reads
            
        # Check SSTables from newest to oldest
        for sstable in reversed(self.sstables):
            self.num_random_reads += 1  # Simulate random I/O to access SSTable
            
            # Binary search in the sorted SSTable
            idx = bisect.bisect_left(sstable, (key,))
            if idx < len(sstable) and sstable[idx][0] == key:
                return sstable[idx][1], self.num_random_reads
                
        return None, self.num_random_reads
        
    def range_query(self, low, high):
        self._reset_counters()
        results = []
        
        # Check memtable
        for key, value in self.memtable.items():
            if low <= key <= high:
                results.append((key, value))
                
        # Check all SSTables
        for sstable in self.sstables:
            self.num_random_reads += 1
            
            # Find start position
            start_idx = bisect.bisect_left(sstable, (low,))
            for i in range(start_idx, len(sstable)):
                key, value = sstable[i]
                if key <= high:
                    results.append((key, value))
                else:
                    break
                    
        # Sort results by key (since they come from multiple sources)
        results.sort(key=lambda x: x[0])
        return results, self.num_random_reads
        
    def force_flush(self):
        """Force flush memtable to SSTable for benchmarking"""
        self._flush_memtable()
