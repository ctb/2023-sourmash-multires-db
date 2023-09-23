#! /usr/bin/env python
import sys
import argparse
import sourmash

from collections import defaultdict


MINIMUM_NUM_HASHES=10


class GroupingBuilder:
    "group by md5 / exact match of minhash contents"
    def __init__(self):
        # track idx <=> group
        self.md5_to_idx_set = defaultdict(set)
        self.idx_to_md5 = {}

        # representative sketch for this grouping
        self.md5_to_sketch = {}

    def add(self, mh, idx):
        assert idx not in self.idx_to_md5

        ss = sourmash.SourmashSignature(mh)
        md5 = ss.md5sum()

        if md5 not in self.md5_to_sketch:
            self.md5_to_sketch[md5] = mh

        self.idx_to_md5[idx] = md5
        self.md5_to_idx_set[md5].add(idx)

    def get_limit_set(self, group_id):
        x = set()
        if group_id is not None:
            x = set(self.md5_to_idx_set[group_id])

        return x

    def items(self, idx_limit_set=None):
        "iterate over all groups, or just those containing idx from limit set"
        if idx_limit_set is None:
            # return everything!
            for md5, minhash in self.md5_to_sketch.items():
                yield md5, minhash
        else:
            # limit to just those idx in the limit set
            print('limiting to:', idx_limit_set)

            md5s = { self.idx_to_md5[idx] for idx in idx_limit_set }
            for md5 in md5s:
                yield md5, self.md5_to_sketch[md5]
            

class Layer:
    """
    A layer represents a complete view of the database at a particular
    scaled, and contains groupings of the sketches at that scaled value.
    """
    def __init__(self, scaled):
        self.scaled = scaled

        # one per layer
        self.groups = GroupingBuilder()


    def add_sketch(self, idx, ss):
        # downsample - this makes sketch specific to this layer
        mh = ss.minhash.downsample(scaled=self.scaled)

        # add to a group: this is where clustering etc could be used
        self.groups.add(mh, idx)

    def best_overlap(self, query, limit_set=None):
        """
        returns best overlap, and idx set of matches

        Searches cluster rep => returns set of matches.s
        """
        query_mh = query.minhash.downsample(scaled=self.scaled)

        i = 0
        best_c = 0
        best_mh = None
        best_group = None

        ## here, probably want to collect all equal matches, not just best CTB
        for group_id, subj_mh in self.groups.items(limit_set):
            i += 1
            c = query_mh.count_common(subj_mh)
            if c >= MINIMUM_NUM_HASHES and c > best_c:
                best_c = c
                best_mh = subj_mh
                best_group = group_id

        print(f'layer{self.scaled} searched {i}')

        new_limit_set = self.groups.get_limit_set(best_group)
        return (best_c, new_limit_set)
            

class MultiResolutionDatabase:
    """
    A MultiResolutionDatabase indexes a database with multiple layers,
    each at a specific scaled value.
    """
    def __init__(self, layers=(1000, 10000, 100000)):
        self.layers = tuple(sorted(layers))
        self.layer_list = []
        for s in layers:
            self.layer_list.append(Layer(s))
        
        self.idx_to_sketches = {}
        self._next_idx = 1

    def add_sketch(self, ss):
        "add a sourmash signature to this database, adding it to all layers"
        assert ss.minhash.scaled == self.layers[0]

        idx = self._next_idx
        self._next_idx += 1

        self.idx_to_sketches[idx] = ss

        for ll in self.layer_list:
            ll.add_sketch(idx, ss)

    def best_overlap(self, query):
        limit_to = None
        for ll in reversed(self.layer_list):
            best_c, x = ll.best_overlap(query, limit_to)
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
        best_c, best_match = self.best_overlap(orig_query)
        query_mh = orig_query.minhash.to_mutable()

        while best_match:
            print('MATCH:', best_c, best_match.name)
            match_mh = best_match.minhash
            query_mh.remove_many(match_mh)

            query = sourmash.SourmashSignature(query_mh)
            best_c, best_match = self.best_overlap(query)


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
