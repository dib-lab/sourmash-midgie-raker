#! /usr/bin/env python
import sys
import os
import argparse
import traceback
import polars as pl

import sourmash


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--query-dbs', nargs='+')
    p.add_argument('--against-db')
    p.add_argument('-k', '--ksize', default=21, type=int)
    p.add_argument('-s', '--scaled', default=1000, type=int)
    p.add_argument('-o', '--output-directory', required=True)
    p.add_argument('--min-hashes', default=5, type=int)
    args = p.parse_args()

    print(f"opening '{args.against_db}'")
    against_db = sourmash.load_file_as_index(args.against_db)
    against_db = against_db.select(ksize=args.ksize, scaled=args.scaled)
    print(f"len: {len(against_db)}")

    threshold_hashes = args.min_hashes
    print(f"setting query threshold: {threshold_hashes}")

    try:
        os.mkdir(args.output_directory)
    except:
        print(f"WARNING: cannot create output directory '{args.output_directory}'. Maybe it already exists? I'm sure it's fine, just fine.")

    for query_db in args.query_dbs:
        print(f"opening query db: '{query_db}'")
        db = sourmash.load_file_as_index(query_db)
        db = db.select(ksize=args.ksize, scaled=args.scaled)
        print(f"len: {len(db)}")

        did = 0
        for n, ss in enumerate(db.signatures()):
            species_name = ss.name.split(' ', 1)[1]
            outname = os.path.join(args.output_directory,
                                   f"{species_name}.parquet")
            if os.path.exists(outname):
                print(f"{outname} exists; skipping")
                continue

            try:
                counter = against_db.counter_gather(ss)
            except:
                print(f"ERROR on species name: {species_name}")
                traceback.print_exc()
                continue

            did += 1
            matches = []
            for (size, name) in counter.matches(threshold_hashes=threshold_hashes):
                matches.append(dict(size=size, acc=name))

            df = pl.DataFrame(matches)
            df.write_parquet(outname)
            print(n, len(db), species_name, len(df))


if __name__ == '__main__':
    sys.exit(main())
