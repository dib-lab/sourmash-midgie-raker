import pprint

configfile: 'config.yaml'
pprint.pprint(config)
            
OUTPUTS=config.get('base')['outputs'].rstrip('/')

###

# @CTB move to config:
GTDB_DB = '/group/ctbrowngrp5/sourmash-db.new/gtdb-rs226/gtdb-rs226-k31.dna.rocksdb'
GTDB_TAX = '/group/ctbrowngrp5/sourmash-db.new/gtdb-rs226/gtdb-rs226.lineages.sqldb'
GTDB_TK_OUTPUT = [
    'inputs/gtdbtk/archeael_tax.gtdbtk.novel.tsv',
    'inputs/gtdbtk/bacterial_tax.gtdbtk.novel.tsv',
    ]

GTDB_ZIP = '/group/ctbrowngrp5/sourmash-db.new/gtdb-rs226/gtdb-rs226-k31.dna.zip'

METAGS=[                        # for explain
    "SRR11125249",
    "SRR12795785",
    "SRR15057925",
    "SRR15057930",
    "SRR5241537",
]

HI_LO_METAGS=[
    "SRR11125249",
    "SRR12795785",
    "SRR15057925",
    "SRR15057930",
    "SRR5241537",
]

RANDOM_10_METAGS=[
    "ERR3211879",
    "SRR8960250",
    "SRR8960946",
    "SRR11126399",
    "SRR8960141",
    "SRR11125447",
    "SRR12795789",
    "ERR8314788",
    "SRR11126199",
    "SRR11125672",
]

EXPLAIN_METAGS=RANDOM_10_METAGS

###

RANKS = 'superkingdom,phylum,class,order,family,genus,species'.split(',')

# sketch the raw bins with their original names
include: "workflows/sketch_raw.smk"

# rename the raw bins into something sensible, if desired
include: "workflows/rename.smk"

# run min-set-cov workflow to explore bin overlap
include: "workflows/min_set_cov.smk"

# run best-bin-containment analysis
include: "workflows/contain.smk"

# run explainability analysis
include: "workflows/explain.smk"

# run rarefaction curve on bins
include: "workflows/rarefy.smk"

rule stage1_fast:
    input:
        rules.sketch_raw.input,
        rules.rename.input,
        rules.rarefaction.input,
