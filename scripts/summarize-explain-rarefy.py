#! /usr/bin/env python
import sys
import argparse
import random
import pandas as pd
def main():
    p = argparse.ArgumentParser()
    p.add_argument('explain_csvs', nargs='+')
    args = p.parse_args()

    x = []
    for filename in args.explain_csvs:
        df = pd.read_csv(filename)
        df = df.sort_values(by=["iteration", "position"])
        summary = df.groupby("position")["cumulative"].agg(["mean", "std"]).reset_index()

        mmax = max(summary["mean"])
        mmin = min(summary["mean"])

        x.append((mmax-mmin, mmin, filename))

    x.sort()

    print('diff mmin filename')
    for diff, mmin, filename in x:
        print(f"{diff:.5f} {mmin:.5f} {filename}")


if __name__ == '__main__':
    sys.exit(main())
    
