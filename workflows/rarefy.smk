# scripts/rarefy-explainability.py --meta inputs/metags/ERR1135207.sig --db outputs.ath/rename/bin-sketches.renamed.sig.zip --scaled=100_000 -o xxx.csv -N 50
SKETCHES = OUTPUTS+'/rename/bin-sketches.renamed.sig.zip'

DEFAULT_KSIZES=[21, 31, 51]
DEFAULT_SCALED = [10_000]
NODEGRAPH_SIZE=100_000_000

KSIZES=config.get('rarefy', {}).get('ksizes', DEFAULT_KSIZES)
SCALED=config.get('rarefy', {}).get('scaled', DEFAULT_SCALED)

rule rarefaction:
    input:
        expand(OUTPUTS+'/rarefy/rarefaction.k{k}.s{s}.csv', k=KSIZES, s=SCALED),
        expand(OUTPUTS+'/rarefy/explain.{m}.k31.csv', m=METAGS),

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

rule metag_explain_curve:
    input:
        metag="inputs/metags/{m}.trim.sig.zip",
        db=SKETCHES,
    output:
        OUTPUTS+'/rarefy/explain.{m}.k31.csv'
    shell: """
        scripts/rarefy-explainability.py --metagenomes {input.metag} \
           --db {input.db} --scaled=10_000 -o {output}
    """
