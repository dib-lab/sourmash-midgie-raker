#! /usr/bin/env python
import sys
import argparse
import random
import pandas as pd


import sourmash
from sourmash.nodegraph import Nodegraph


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--metagenomes', nargs='+')
    p.add_argument('--db', nargs='+')
    p.add_argument('--scaled', default=1000, type=int)
    p.add_argument('-k', '--ksize', default=31)
    p.add_argument('--nodegraph-size', type=int, default=10_000_000)
    p.add_argument('--nodegraph-num', type=int, default=4)
    p.add_argument('-N', '--num-iterations', type=int, default=10)
    p.add_argument('-o', '--output', required=True)
    args = p.parse_args()

    metags = []
    for filename in args.metagenomes:
        print(f'loading metagenomes from {filename}')
        sigs = sourmash.load_file_as_signatures(filename,
                                                ksize=args.ksize)
        for metag in sigs:
            metag_mh = metag.minhash.downsample(scaled=args.scaled)
            metags.append((metag.name, metag_mh))

    print(f'loaded {len(metags)} metagenomes')

    mhlist = []
    for dbname in args.db:
        print(f'loading sketches from {dbname}')
        db = sourmash.load_file_as_index(dbname)
        db = db.select(ksize=args.ksize)
        for ss in db.signatures():
            mh = ss.minhash.downsample(scaled=args.scaled)
            mhlist.append(mh)

            if len(mhlist) % 1000 == 0:
                print(f'... loaded {len(mhlist)}')

    print(f'loaded {len(mhlist)} sketches total.')

    print(f'running {args.num_iterations} iterations of shuffled genomes')
    all_results = []
    for i in range(args.num_iterations):
        random.shuffle(mhlist)
        ng = Nodegraph(args.ksize, args.nodegraph_size, args.nodegraph_num)
        for n, mh in enumerate(mhlist):
            ng.update(mh)
            if n % 100 == 0:
                for (name, metag_mh) in metags:
                    total = 0
                    found = 0
                    for hashval, abund in metag_mh.hashes.items():
                        total += abund
                        if ng.get(hashval):
                            found += abund

                    wcont = round(found / total, 6)
                    print(i, n, name, wcont)

                    all_results.append({
                        "iteration": i,
                        "position": n,
                        "cumulative": wcont,
                        "metag": name,
                    })

    # Convert to pandas DataFrame
    df = pd.DataFrame(all_results)

    # Save to CSV
    df.to_csv(args.output, index=False)
    


if __name__ == '__main__':
    sys.exit(main())
