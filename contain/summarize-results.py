#! /usr/bin/env python
import sys
import csv
import argparse
from collections import defaultdict
import math


def main():
    p = argparse.ArgumentParser()
    p.add_argument('results_csv', nargs='+')
    p.add_argument('--min-containment', default=.1, type=float)
    p.add_argument('-o', '--output-summary', required=True)
    args = p.parse_args()

    results = []
    by_mag = defaultdict(list)
    by_mag_maxcont = defaultdict(float)
    for filename in args.results_csv:
        with open(filename, "r", newline='') as fp:
            dd = defaultdict(list)
            r = csv.DictReader(fp)
            magident = None
            for row in r:
                # filter on min-containment
                cont = float(row['containment'])
                if cont < args.min_containment:
                    continue

                # store results by metagenome
                metag = row['match_name']
                dd[metag].append(row)

                # retrieve ident - is it a GTDB genome, or a MAG??
                query_name = row['query_name']
                ident = query_name.split(' ')[0]

                metag_in_name = f"_{metag}" in query_name
                if metag_in_name: # skip? make optional @CTB
                    print('SKIP', metag, query_name)
                    continue

                if not ident.startswith('GC'):
                    if magident is None:
                        magident = ident
                        by_mag_maxcont[ident] = cont
                    else:
                        # should only be one non-GTDB MAG per manysearch file
                        assert magident == ident

            # no MAG matches, skip.
            if magident is None:
                continue

            for metag, rows in dd.items():
                rows = sorted(rows, key=lambda x: -float(x['containment']))
                best_gtdb = 0
                magval = None
                for row in rows:
                    if row['query_name'].startswith('GC'):
                        best_gtdb = float(row['containment'])
                    elif row['query_name'].startswith(magident):
                        magval = float(row['containment'])

                diff = None
                if magval:
                    diff = magval - best_gtdb
                    results.append((metag, magident, diff))

    for metag, magident, diff in results:
        by_mag[magident].append(diff)

    results2 = []
    for mag, diffs in by_mag.items():
        avg = sum(diffs) / len(diffs)
        dev = math.sqrt(sum([ (d - avg)**2 for d in diffs ]))
        results2.append((avg, dev, mag))

    results2.sort(reverse=True)
    #for avg, dev, mag in results2:
    #    print(f"{mag} {avg:.4f} {dev:.4f}")

    with open(args.output_summary, 'w', newline='') as fp:
        w = csv.writer(fp)
        w.writerow('mag,mean_advantage,std_advantage,max_cont'.split(','))
        for avg, dev, mag in results2:
            w.writerow([mag, avg, dev, by_mag_maxcont[mag]])


if __name__ == '__main__':
    sys.exit(main())
