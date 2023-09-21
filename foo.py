#! /usr/bin/env python
import sys
import argparse
import sourmash

from collections import defaultdict


class Layer:
    def __init__(self, scaled):
        self.scaled = scaled
        self.sketches_to_idx_set = defaultdict(set)
        self.idx_to_md5 = {}
        self.md5_to_sketch = {}
        self.sketches = []

    def add_sketch(self, idx, ss):
        assert idx not in self.idx_to_md5

        mh = ss.minhash.downsample(scaled=self.scaled)
        ss2 = sourmash.SourmashSignature(mh)
        md5 = ss2.md5sum()
        if md5 not in self.md5_to_sketch:
            self.md5_to_sketch[md5] = mh

        self.sketches_to_idx_set[md5].add(idx)
        self.idx_to_md5[idx] = md5

    def best_containment(self, query, limit_set=None):
        "returns best containment and idx set of matches"
        query_mh = query.minhash.downsample(scaled=self.scaled)

        i = 0
        best_c = 0
        best_mh = None
        best_md5 = None

        if limit_set:
            md5s = { self.idx_to_md5[idx] for idx in limit_set }
            sketches_to_search = [ (md5, self.md5_to_sketch[md5]) for md5 in md5s ]
            print('limiting to:', limit_set, len(sketches_to_search))
        else:
            sketches_to_search = self.md5_to_sketch.items()
        
        for (subj_md5, subj_mh) in sketches_to_search:
            i += 1
            c = query_mh.count_common(subj_mh)
            if c > best_c:
                best_c = c
                best_mh = subj_mh
                best_md5 = subj_md5

        x = set()
        if best_mh is not None:
            x = set(self.sketches_to_idx_set[best_md5])

        print(f'layer{self.scaled} searched {i}')

        return (best_c, x)
            

class MultiResolutionDatabase:
    def __init__(self, layers=(1000, 10000, 100000)):
        self.layers = tuple(sorted(layers))
        self.layer_list = []
        for s in layers:
            self.layer_list.append(Layer(s))
        
        self.idx_to_sketches = {}
        self._next_idx = 1

    def add_sketch(self, ss):
        assert ss.minhash.scaled == self.layers[0]

        idx = self._next_idx
        self._next_idx += 1

        self.idx_to_sketches[idx] = ss

        for ll in self.layer_list:
            ll.add_sketch(idx, ss)


    def best_containment(self, query):
        limit_to = None
        for ll in reversed(self.layer_list):
            best_c, x = ll.best_containment(query, limit_to)
            print(f'best match layer{ll.scaled}: {best_c} limit={x}')
            if x:
                limit_to = x

        if x:
            assert best_c > 0
            best_match_idx = x.pop()
            best_match = self.idx_to_sketches[best_match_idx]

            return best_c, best_match
        else:
            return 0, None

    def gather(self, orig_query):
        best_c, best_match = self.best_containment(orig_query)
        query_mh = orig_query.minhash.to_mutable()

        while best_match:
            print('MATCH:', best_c, best_match.name)
            match_mh = best_match.minhash
            query_mh.remove_many(match_mh)

            query = sourmash.SourmashSignature(query_mh)
            best_c, best_match = self.best_containment(query)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('query')
    p.add_argument('zipfile_db')
    p.add_argument('-s', '--min-scaled', default=1000)
    p.add_argument('-M', '--max-scaled', default=100000)
    p.add_argument('-k', '--ksize', default=31)
    args = p.parse_args()

    query = sourmash.load_file_as_index(args.query,)
    query = query.select(ksize=args.ksize, scaled=args.min_scaled)
    query = list(query.signatures())
    assert len(query) == 1, len(query)
    query = query[0]

    idx = sourmash.load_file_as_index(args.zipfile_db)
    idx = idx.select(ksize=args.ksize, scaled=args.min_scaled)

    mrd = MultiResolutionDatabase()

    for ss in idx.signatures():
        mrd.add_sketch(ss)

    mrd.gather(query)


if __name__ == '__main__':
    sys.exit(main())
