#! /usr/bin/env python
"""
do a gather with the MultiResolutionDB
"""
import sys
import argparse
import sourmash

from multires_db import MultiResolutionDatabase


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
