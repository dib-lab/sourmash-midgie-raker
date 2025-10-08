#! /usr/bin/env python
import argparse
import sys

import sourmash
from pathlib import Path
from sourmash.nodegraph import Nodegraph
import pandas as pd
import random


def main():
    p = argparse.ArgumentParser()
    p.add_argument('sketches', nargs='+')
    p.add_argument('-k', '--ksize', type=int, default=31)
    p.add_argument('-N', '--num-iterations', type=int, default=100)
    p.add_argument('-o', '--output', required=True)
    p.add_argument('-s', '--scaled', default=1000, type=int)
    p.add_argument('--nodegraph-size', type=int, default=10_000_000)
    p.add_argument('--nodegraph-num', type=int, default=4)
    args = p.parse_args()

    # Parameters
    ksize = args.ksize
    n_iterations = args.num_iterations

    # load sketches
    sketches = []
    for filename in args.sketches:
        db = sourmash.load_file_as_index(filename)
        db = db.select(ksize=ksize)
        print(f'found {len(db)} sketches at k={ksize} - loading as signatures now.')
        for ss in db.signatures():
            with ss.update() as ss:
                ss.minhash = ss.minhash.downsample(scaled=args.scaled)
            sketches.append(ss)

            if len(sketches) % 100 == 0:
                print('...', len(sketches))

    print('...done!')

    # Prepare a list to store results for all iterations
    all_results = []

    for iteration in range(n_iterations):
        print(f"Iteration {iteration + 1}/{n_iterations}")

        # Shuffle sample order for this iteration
        shuffled_sigs = list(sketches)
        random.shuffle(shuffled_sigs)

        # Reset Nodegraph for each iteration
        all_hashes = Nodegraph(ksize, args.nodegraph_size, args.nodegraph_num)

        cumulative_kmers = 0
        for position, ss in enumerate(shuffled_sigs, start=1):
            prev_total = all_hashes.n_occupied()
            all_hashes.update(ss.minhash)
            new_total = all_hashes.n_occupied()
            new_kmers = new_total - prev_total

            # Append a row with iteration, sample, position in this iteration, new kmers, cumulative total
            all_results.append({
                "iteration": iteration,
                "position": position,
                "sample": ss.name,
                "new_kmers": new_kmers,
                "cumulative_kmers": new_total
            })

    if all_hashes.n_occupied() > 0.2 * args.nodegraph_size:
       print('WARNING: individual nodegraphs have high occupancy', all_hashes.n_occupied())
       sys.exit(-1)

    # Convert to pandas DataFrame
    df = pd.DataFrame(all_results)

    # Save to CSV
    df.to_csv(args.output, index=False)


if __name__ == '__main__':
    sys.exit(main())
