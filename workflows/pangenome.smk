rule pangenomedb:
    input:
        OUTPUTS+'/rename/gtdb+bins.species.sig.zip',
        SHARED+'/gtdb.species.sig.zip',

rule gtdb_bins_mf:
    input:
        bins=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        gtdb=GTDB_ZIP,
    output:
        OUTPUTS+'/rename/gtdb+bins.mf.csv',
    shell: """
        sourmash sig collect -F csv -o {output} {input} --abspath
    """

rule gtdb_bins_species_pangenome:
    input:
        mf=OUTPUTS+'/rename/gtdb+bins.mf.csv',
        tax=[OUTPUTS+'/rename/bin-sketches.lineages.csv',
             GTDB_TAX],
    output:
        OUTPUTS+'/rename/gtdb+bins.species.sig.zip',
    shell: """
        sourmash scripts pangenome_createdb {input.mf} -t {input.tax} \
           -k 31 -o {output}
    """
        
rule gtdb_species_pangenome:
    input:
        zip=GTDB_ZIP,
        tax=GTDB_TAX,
    output:
        SHARED+'/gtdb.species.sig.zip',
    shell: """
        sourmash scripts pangenome_createdb {input.zip} -t {input.tax} \
           -k 31 -o {output}
    """
