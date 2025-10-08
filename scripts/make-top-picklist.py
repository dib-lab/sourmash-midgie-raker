#! /usr/bin/env python
import csv
import sys
import argparse
import collections
import itertools


def main():
    p = argparse.ArgumentParser()
    p.add_argument('fastgather_csv')
    p.add_argument('-n', '--num-matches', default=10, type=int)
    args = p.parse_args()

    dd = collections.defaultdict(list)
    with open(args.fastgather_csv, 'r', newline='') as fp:
        r = csv.DictReader(fp)
        for row in r:
            query_name = row['query_name']
            dd[query_name].append(row)

    print(f'loaded results for {len(dd)} queries.')

    for query_name, rows in dd.items():
        rows = sorted(rows, key=lambda row: -float(row['f_match_orig']))
        rows = itertools.islice(rows, args.num_matches)

        idents = [ row['match_name'].split(' ')[0] for row in rows ]

        query_ident = query_name.split(' ')[0]

        with open(f'{query_ident}.topN.pl.csv', 'w', newline='') as fp:
            w = csv.writer(fp)
            w.writerow(['ident'])
            w.writerow([query_ident])
            for ident in idents:
                w.writerow([ident])

    print(f'wrote {len(dd)} picklists.')


if __name__ == '__main__':
    sys.exit(main())
