SKETCHES = OUTPUTS+'/rename/bin-sketches.renamed.sig.zip'

DEFAULT_KSIZES=[21, 31, 15]
DEFAULT_SCALED = [10_000]
NODEGRAPH_SIZE=100_000_000

KSIZES=config.get('rarefy').get('ksizes', DEFAULT_KSIZES)
SCALED=config.get('rarefy').get('scaled', DEFAULT_SCALED)

rule rarefaction:
    input:
        expand(OUTPUTS+'/rarefy/rarefaction.k{k}.s{s}.csv', k=KSIZES, s=SCALED)

rule make_rarefy:
    input:
        SKETCHES,
    output:
        OUTPUTS+'/rarefy/rarefaction.k{ksize}.s{scaled}.csv'
    shell: """
        ./scripts/rarefy.py {input} -k {wildcards.ksize} \
            --scaled {wildcards.scaled} -o {output} \
            --nodegraph-size {NODEGRAPH_SIZE}
    """
