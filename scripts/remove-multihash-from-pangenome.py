#! /usr/bin/env python
import sys
import argparse
import sourmash
from collections import Counter
from sourmash.sourmash_args import SaveSignaturesToLocation


def main():
    p = argparse.ArgumentParser()
    p.add_argument('pangenome_db')
    p.add_argument('-k', '--ksize', default=31)
    p.add_argument('-o', '--output', required=True)
    args = p.parse_args()

    db = sourmash.load_file_as_index(args.pangenome_db)
    db = db.select(ksize=args.ksize)

    print('XXX', len(db))

    # load and count.
    cnt = Counter()
    for n, ss in enumerate(db.signatures()):
        if n % 1000 == 0:
            print('...', n)
        mh = ss.minhash.flatten()
        cnt.update(mh.hashes)

    # calculate all non-singletons
    nonsingletons = set()
    for hashval, count in cnt.most_common():
        if count < 2:
            break
        nonsingletons.add(hashval)

    # print out counts
    print(len(nonsingletons), len(cnt))

    # remove non-singletons
    with SaveSignaturesToLocation(args.output) as save_sig:
        for n, ss in enumerate(db.signatures()):
            if n % 1000 == 0:
                print('writing ...', n)

            mh = ss.minhash
            hashes = set(ss.minhash.hashes)
            hashes -= nonsingletons
            mh = mh.copy_and_clear()
            mh.add_many(hashes)

            ss = sourmash.SourmashSignature(mh, name=ss.name)
            save_sig.add(ss)


if __name__ == '__main__':
    sys.exit(main())
