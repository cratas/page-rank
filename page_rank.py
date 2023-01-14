from collections import defaultdict
from multiprocessing import Pool, Manager
import itertools as it
import multiprocessing as mp
import time

NUM_WORKERS = mp.cpu_count()

class PageRank():
    def __init__(self, fname):
        self.fname = fname

    def load_data(self):
        edges = dict()

        num_lines = sum(1 for line in open(self.fname))
        chunk_size = num_lines // NUM_WORKERS

        with Pool(NUM_WORKERS) as p:
            edges = p.map(self.read_file, [(self.fname, pid, chunk_size) for pid in range(NUM_WORKERS)])
        
        return self.merge_dict(edges)

    def merge_dict(self, original_dict):
        merged_dict = defaultdict(list)

        for d in original_dict:
            for k, v in d.items():
                merged_dict[k] += v

        return dict(merged_dict)

    def read_file(self, args):
        fname, pid, chunk_size = args
        start = pid * chunk_size
        end = start + chunk_size

        edges = dict()
        with open(fname) as f:
            for line in it.islice(f, start, end):
                if not line.startswith("#"):
                    from_node, to_node = map(int, line.strip().split())
                    edges.setdefault(from_node, []).append(to_node)

        return edges

if __name__ == '__main__':
    pr = PageRank('web-BerkStan.txt')
    
    st = time.time()
    sparse_matrix = pr.load_data() # creating sparse matrix
    et = time.time()

    elapsed_time = et - st
    print('Execution time:', elapsed_time, 'seconds')