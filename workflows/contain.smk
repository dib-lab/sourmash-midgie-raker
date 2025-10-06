MAGS, = glob_wildcards(OUTPUTS+'/contain/picklists/{mag}.topN.pl.csv')

print(f'found {len(MAGS)} MAGs')

## cd outputs.test/contain/picklists
## ../../../scripts/make-top-picklist.py ../../min-set-cov/sketches.x.gtdb.fastgather.csv
## cd -

rule contain:
    input:
        expand(OUTPUTS + '/contain/results/{ident}.topN.manysearch.csv', ident=MAGS)

rule contain_check:
    input:
        pl=OUTPUTS+'/contain/picklists/{ident}.topN.pl.csv',
        sketches=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
    output:
        OUTPUTS+'/contain/manifests/{ident}.topN.mf.csv',
    shell: """
        sourmash sig check -m {output} --picklist {input.pl}:ident:ident \
             {input.sketches} {GTDB_ZIP} \
             -k 31 --abspath -F csv
    """

rule contain_collect_metags:
    input:
        expand('inputs/metags/{name}.trim.sig.zip', name=METAGS)
    output:
        OUTPUTS+'/contain/metags.mf.csv',
    shell:
        "sourmash sig collect {input} -o {output} -F csv --abspath"

rule manysearch:
    input:
        query_mf=OUTPUTS+'/contain/manifests/{ident}.topN.mf.csv',
        metags=OUTPUTS+'/contain/metags.mf.csv',
    output:
        OUTPUTS+'/contain/results/{ident}.topN.manysearch.csv',
    threads: 4
    shell: """
        sourmash scripts manysearch -c {threads} -t 0 \
           {input.query_mf} {input.metags} -o {output}
    """
