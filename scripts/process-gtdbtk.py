#! /usr/bin/env python
import sys
import argparse
import csv
from collections import defaultdict


RANKS = 'superkingdom,phylum,class,order,family,genus,species'.split(',')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('gtdbtk_tsvs', nargs='*')
    p.add_argument('--allow-empty', action='store_true')
    p.add_argument('-o', '--output-lineages-csv')
    args = p.parse_args()

    classify_d = {}
    new_rank_num = defaultdict(int)
    for filename in args.gtdbtk_tsvs:
        with open(filename, 'r', newline='') as fp:
            r = csv.DictReader(fp, delimiter='\t')
            if 'user_genome' not in r.fieldnames:
                print(f"ERROR: {filename} does not appear to be a GTDB-TK TSV file.")
                sys.exit(-1)

            rows = list(r)

            for row in rows:
                ident = row['user_genome'].split(' ', 1)[0]
                assert ident not in classify_d
                classify_d[ident] = row

    print(f"loaded {len(classify_d)} rows from {len(args.gtdbtk_tsvs)} file(s)")

    if len(classify_d) == 0 and not args.allow_empty:
        print('no custom lineages found')
        sys.exit(-1)

    lineages_d = {}
    for ident, row in classify_d.items():
        classify = row['classification']
        classify = classify.split(';')
        classify = [ x for x in classify if len(x) > 3 ]

        assert len(classify)
        if len(classify) == len(RANKS):
            lineages_d[ident] = classify
            continue

        n_ranks = len(classify) - 1
        for rank in RANKS[n_ranks + 1:-1]:
            new_rank_num[rank] += 1
            next_rank_num = new_rank_num[rank]

            name = rank[0] + '__' + f'novel_{rank}_{next_rank_num}'
            classify.append(name)

        # do species
        genus = classify[-1]
        assert genus.startswith('g__'), genus
        new_rank_num['species'] += 1
        next_species_num = new_rank_num['species']
        name = f's__{genus[2:]} novel_{next_species_num}'
        classify.append(name)

        assert len(classify) == len(RANKS)
        lineages_d[ident] = classify

    if args.output_lineages_csv:
        print(f"writing {len(lineages_d)} to '{args.output_lineages_csv}'")
        with open(args.output_lineages_csv, 'w', newline='') as fp:
            w = csv.writer(fp)
            w.writerow(['ident', 'lineage'])
            for ident, lintup in lineages_d.items():
                w.writerow([ident, ";".join(lintup)])


if __name__ == '__main__':
    sys.exit(main())
