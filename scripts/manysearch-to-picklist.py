#! /usr/bin/env python
import sys
import argparse
import polars as pl


def main():
    p = argparse.ArgumentParser()
    p.add_argument('manysearch_csvs', nargs='+')
    p.add_argument('-o', '--output', required=True)
    args = p.parse_args()

    df_list = []
    for csvfile in args.manysearch_csvs:
        df = pl.read_csv(csvfile)
        df_list.append(df)
    print('loaded {len(df_list)} data frames')

    idents = set()
    for df in df_list:
        for name in df['query_name'].to_list():
            ident = name.split(' ')[0]
            idents.add(ident)

    print('got {len(idents)} idents')

    with open(args.output, 'wt') as fp:
        fp.write("ident\n")
        fp.write("\n".join(idents))


if __name__ == '__main__':
    sys.exit(main())
