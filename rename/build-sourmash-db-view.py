#! /usr/bin/env python
import sys
import argparse
from collections import defaultdict

import sourmash
import taxburst
import sourmash_utils
from sourmash.tax import tax_utils


def main():
    p = argparse.ArgumentParser()
    p.add_argument("sourmash_db", nargs="+")
    p.add_argument(
        "-t", "--taxonomy", required=True, help="sourmash lineages file(s)", nargs="+"
    )
    p.add_argument("-o", "--output-html", help="output HTML file to this location.")
    p.add_argument("--save-json", help="output a JSON file of the taxonomy")
    p.add_argument("--name", help="name of tree", default=None)
    p.add_argument(
        "--max-rank-num",
        default=6,
        type=int,
        help="maximum number of ranks to include in output",
    )

    sourmash_utils.add_standard_minhash_args(p)
    args = p.parse_args()

    if not args.output_html and not args.save_json:
        print(f"No output specified?! Error exit.")
        sys.exit(-1)

    name = args.name
    if name is None:
        name = args.output_html or args.save_json
        name = name.rsplit(".", 1)[0]
    print(f"output tree name is: {name}; use --name to override.")

    template_mh = sourmash_utils.create_minhash_from_args(args)

    taxdb = sourmash.tax.tax_utils.MultiLineageDB.load(args.taxonomy)
    print(f"loaded {len(taxdb)} lineages from taxonomy files.")

    sig_to_info = {}
    for dbname in args.sourmash_db:
        db = sourmash_utils.load_index_and_select(dbname, template_mh)
        mf = db.manifest
        for row in mf.rows:
            name = row["name"]
            assert name not in sig_to_info
            sig_to_info[row["name"]] = row

    print(f"loaded {len(sig_to_info)} sketches from sourmash databases.")

    # aggregate rows across all sublineages up to root
    rows_by_tax = defaultdict(list)
    for name, row in sig_to_info.items():
        ident = name.split(".")[0].split(' ')[0]
        if ident in taxdb:
            lineage = taxdb[ident]

            for i in range(1, len(lineage) + 1):
                lin = lineage[:i]
                rows_by_tax[lin].append(row)
        else:
            lin = ("unclassified",)
            rows_by_tax[lin].append(row)

    # build nodes & assign to tax
    nodes_by_tax = {}
    for lin, rows in rows_by_tax.items():
        if lin == ("unclassified",):
            name = "unclassified"
            rank = "superkingdom"
        else:
            rank = lin[-1].rank
            name = lin[-1].name

        count = len(rows)
        node = dict(name=name, rank=rank, count=count)
        nodes_by_tax[lin] = node

    # filter out those below max rank
    nodes_by_tax2 = {}
    print(f"filtering at rank {lin[args.max_rank_num - 1].rank}")
    for lin, rows in nodes_by_tax.items():
        if len(lin) <= args.max_rank_num:
            nodes_by_tax2[lin] = rows
    print(f"kept {len(nodes_by_tax2)} of {len(nodes_by_tax)} total nodes.")
    nodes_by_tax = nodes_by_tax2

    # assign children
    children_by_lin = defaultdict(list)
    top_nodes = []
    for lin, node in nodes_by_tax.items():
        if len(lin) == 1:  # top node; no parents
            top_nodes.append(node)
            assert node["rank"] == "superkingdom"
            continue

        # assign node to parent in children_by_lin.
        parent_lin = lin[:-1]
        children_by_lin[parent_lin].append(node)

    # now go back through and assign children to parent.
    for lin, node in nodes_by_tax.items():
        children = children_by_lin[lin]
        node["children"] = children

    # test the resulting structure:
    taxburst.checks.check_structure(top_nodes)
    taxburst.checks.check_all_counts(top_nodes, fail_on_error=True)

    if args.save_json:
        print(f"saving tree in JSON format to '{args.save_json}'")
        with open(args.save_json, "wt") as fp:
            json.dump(top_nodes, fp)

    # build XHTML
    content = taxburst.generate_html(top_nodes, name="tree")  # @CTB

    # output!!
    if args.output_html:
        with open(args.output_html, "wt") as fp:
            fp.write(content)

        print(f"wrote output to '{args.output_html}'")


if __name__ == "__main__":
    sys.exit(main())
