#! /usr/bin/env python
import sys
import argparse
import sourmash
from collections import Counter
import csv


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sigs', nargs='+')
    p.add_argument('-o', '--output', required=True)
    p.add_argument('-m', '--minimum-count', type=int, default=10)
    args = p.parse_args()

    cnt = Counter()
    n_sketches = 0
    for filename in args.sigs:
        for sketches in sourmash.load_file_as_signatures(filename):
            n_sketches += 1
            mh = sketches.minhash
            for hashval in mh.hashes:
                cnt[hashval] += 1

    with open(args.output, 'w', newline='') as fp:
        w = csv.writer(fp)
        wrote = 0
        for hashval, count in cnt.most_common():
            w.writerow([hashval, count, count / n_sketches])
            wrote += 1
            if count < args.minimum_count:
                break
    print(f"wrote {wrote} hashvals from {n_sketches} sketches to '{args.output}'")


if __name__ == '__main__':
    sys.exit(main())
