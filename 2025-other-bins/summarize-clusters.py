#! /usr/bin/env python
import argparse
import sys
import csv
import collections

csv.field_size_limit(sys.maxsize)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('cluster_csv')
    args = p.parse_args()

    with open(args.cluster_csv, 'r', newline='') as fp:
        r = csv.DictReader(fp)
        rows = list(r)

    print(f"loaded {len(rows)} clusters from '{args.cluster_csv}'")

    cnt = collections.Counter()
    for row in rows:
        nodes = row['nodes'].split(';')

        origins = []
        for n in nodes:
            ident = None
            if n.startswith('NNF_'):
                ident = n.split('_')[1]
            elif n.startswith('AtH_'):
                ident = 'AtH'
            elif n.startswith('DG_'):
                ident = 'DG'
            elif n.startswith('UPGG_'):
                ident = 'UPGG'
            else:
                assert 0, n
            origins.append(ident)
            
        origins = set(origins)
        origins = tuple(sorted(origins))
        cnt[origins] += 1

    print('membership                                                PCNT    count of bins')
    print('----------                                                ----    -------------')
    sofar = 0
    for origins, count in cnt.most_common():
        num = len(origins)
        og = ";".join(origins)
        pcnt = count / len(rows) * 100
        sofar += pcnt

        print(f"in {num}: {og:50}  {pcnt:>4.1f}% ({sofar:>4.1f}%)     {count}")
    

if __name__ == '__main__':
    sys.exit(main())
