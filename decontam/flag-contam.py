#! /usr/bin/env python
import csv
import sys
import argparse
import collections

RANKS = 'superkingdom,phylum,class,order,family,genus,species'.split(',')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('gather_with_lineages_csvs', nargs='+')
    p.add_argument('-r', '--fail-rank', default='class')
    p.add_argument('-m', '--min-fraction', default=0.95, type=float)
    p.add_argument('-v', '--verbose', action='store_true')
    args = p.parse_args()

    query_d = collections.defaultdict(list)

    for filename in args.gather_with_lineages_csvs:
        fp = open(filename, 'r', newline='')
        r = csv.DictReader(fp)

        for row in r:
            query = row['query_name']
            query_d[query].append(row)

    print(f'loaded gather info for {len(query_d)} gathers')

    first_rows = next(iter(query_d.values()))
    row = first_rows[0]

    colname = 'name'
    if 'match_name' in row:
        colname = 'match_name'

    rank_idx = RANKS.index(args.fail_rank)

    for query_name, rows in query_d.items():
        lineages = [ (row['lineage'], row['f_unique_to_query']) for row in rows ]

        linsum = collections.defaultdict(float)
        for lin, fraction in lineages:
            # pull out the path to the given rank
            lin = lin.split(';')[:rank_idx+1]
            lin = tuple(lin)

            # sum fraction across all of this lineage
            linsum[lin] += float(fraction)

        total_known_fraction = sum(linsum.values())

        items = list(linsum.items())
        items.sort(key=lambda x: -x[1])

        domlin, domfrac = items[0]
        domfrac /= total_known_fraction
        if args.verbose:
            print(f'{query_name}: f_known for dominant lin {domlin[-1]}: {domfrac:0.3f}')
        if domfrac > args.min_fraction:
            pass
        else:
            ident = query_name.split(' ')[0]
            print(f'{ident}: contaminated. dominant lineage is only {domfrac:0.3f} of total known.')


if __name__ == '__main__':
    sys.exit(main())

    
