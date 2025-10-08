import pprint

configfile: 'databases.yaml'
configfile: 'config.yaml'
pprint.pprint(config)
            
OUTPUTS=config.get('base')['outputs'].rstrip('/')
GTDB_DB=config.get('databases')['gtdb_rocksdb']
GTDB_TAX=config.get('databases')['gtdb_tax']
GTDB_ZIP=config.get('databases')['gtdb_zip']

###

# @CTB move to config:

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

## summary rules for convenience:

rule stage1_fast:
    input:
        rules.sketch_raw.input,
        rules.rename.input,
        rules.rarefaction.input,
