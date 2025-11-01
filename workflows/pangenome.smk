rule pangenomedb:
    input:
        OUTPUTS+'/rename/gtdb+bins.species.sig.zip',
        OUTPUTS+'/rename/gtdb+bins.species.k31.rocksdb',
        SHARED+'/gtdb.species.sig.zip',
        SHARED+'/gtdb.species.k31.rocksdb',
        SHARED+'/gtdb-merged.species.sig.zip',
        expand(OUTPUTS+'/pangenome/{metag}.gtdb+bins.species.gather.csv',
               metag=EXPLAIN_METAGS),
        expand(OUTPUTS+'/pangenome/{metag}.gtdb.species.gather.csv',
               metag=EXPLAIN_METAGS),

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

rule gtdb_merged_species_pangenome:
    input:
        SHARED+'/gtdb.species.sig.zip',
    output:
        SHARED+'/gtdb-merged.species.sig.zip',
    shell: """
        sourmash sig downsample {input} --scaled=10_000 | sourmash sig merge - \
            -o {output} --set-name gtdb-merged
    """

rule gtdb_bins_species_gather:
    input:
        metag='inputs/metags/{metag}.sig',
        db=OUTPUTS+'/rename/gtdb+bins.species.k31.rocksdb',
    output:
        csv=OUTPUTS+'/pangenome/{metag}.gtdb+bins.species.gather.csv',
        out=OUTPUTS+'/pangenome/{metag}.gtdb+bins.species.gather.out',
    shell: """
        sourmash gather -k 31 --scaled 10_000 --threshold-bp=0 \
            {input.metag} {input.db} -o {output.csv} > {output.out}
    """

rule gtdb_species_gather:
    input:
        metag='inputs/metags/{metag}.sig',
        db=SHARED+'/gtdb.species.k31.rocksdb',
    output:
        csv=OUTPUTS+'/pangenome/{metag}.gtdb.species.gather.csv',
        out=OUTPUTS+'/pangenome/{metag}.gtdb.species.gather.out'
    shell: """
        sourmash gather -k 31 --scaled 10_000 --threshold-bp=0 \
            {input.metag} {input.db} -o {output.csv} > {output.out}
    """

rule rocksdb_k31:
    input:
        "{location}.sig.zip",
    output:
        directory("{location}.k31.rocksdb"),
    shell: """
        sourmash index -F rocksdb -k 31 {output} {input}
    """

