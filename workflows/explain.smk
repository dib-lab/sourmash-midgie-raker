rule explain:
    input:
        expand(OUTPUTS+"/explain/{metag}.gtdb+bins.gather.csv", metag=EXPLAIN_METAGS),
        expand(OUTPUTS+"/explain/{metag}.gtdb-only.gather.csv", metag=EXPLAIN_METAGS),
        expand(OUTPUTS+"/explain/{metag}.bins-only.gather.csv", metag=EXPLAIN_METAGS),

rule explain_index:
    input:
        SKETCHES,
    output:
        directory(OUTPUTS+'/bin-sketches.k31.rocksdb'),
    shell: """
        sourmash index -F rocksdb {output} {input} -k 31
    """

rule explain_gather:
    input:
        q="inputs/metags/{metag}.sig",
        rocksdb=OUTPUTS+'/bin-sketches.k31.rocksdb',
    output:
        OUTPUTS+"/explain/{metag}.gtdb+bins.gather.csv"
    threads: 1
    shell: """
        sourmash gather {input.q} {GTDB_DB} {input.rocksdb} -o {output} \
           -k 31 --threshold-bp=0 --scaled 10_000
    """

rule explain_gather_bins:
    input:
        q="inputs/metags/{metag}.sig",
        rocksdb=OUTPUTS+'/bin-sketches.k31.rocksdb',
    output:
        OUTPUTS+"/explain/{metag}.bins-only.gather.csv"
    threads: 1
    shell: """
        sourmash gather {input.q} {input.rocksdb} -o {output} \
           -k 31 --threshold-bp=0 --scaled 10_000
    """

rule explain_gather_gtdb:
    input:
        "inputs/metags/{metag}.sig"
    output:
        OUTPUTS+"/explain/{metag}.gtdb-only.gather.csv"
    threads: 1
    shell: """
        sourmash gather {input} {GTDB_DB} -o {output} \
           -k 31 --threshold-bp=0 --scaled 10_000
    """
