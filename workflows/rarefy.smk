SKETCHES = OUTPUTS+'/rename/bin-sketches.renamed.sig.zip'

KSIZES=[21,31,51]
SCALED=10_000

rule rarefaction:
    input:
        expand(OUTPUTS+'/rarefy/rarefaction.k{k}.csv', k=KSIZES)

rule make_rarefy:
    input:
        SKETCHES,
    output:
        OUTPUTS+'/rarefy/rarefaction.k{ksize}.csv'
    shell: """
        ./scripts/rarefy.py {input} -k {wildcards.ksize} --scaled {SCALED} \
            -o {output}
    """
