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
        csv=OUTPUTS+"/explain/{metag}.gtdb+bins.gather.csv",
        txt=OUTPUTS+"/explain/{metag}.gtdb+bins.gather.out",
    threads: 1
    shell: """
        sourmash gather {input.q} {GTDB_DB} {input.rocksdb} -o {output.csv} \
           -k 31 --threshold-bp=0 --scaled 10_000 >& {output.txt}
    """

rule explain_gather_bins:
    input:
        q="inputs/metags/{metag}.sig",
        rocksdb=OUTPUTS+'/bin-sketches.k31.rocksdb',
    output:
        csv=OUTPUTS+"/explain/{metag}.bins-only.gather.csv",
        txt=OUTPUTS+"/explain/{metag}.bins-only.gather.out",
    threads: 1
    shell: """
        sourmash gather {input.q} {input.rocksdb} -o {output.csv} \
           -k 31 --threshold-bp=0 --scaled 10_000 >& {output.txt}
    """

rule explain_gather_gtdb:
    input:
        "inputs/metags/{metag}.sig"
    output:
        csv=OUTPUTS+"/explain/{metag}.gtdb-only.gather.csv",
        txt=OUTPUTS+"/explain/{metag}.gtdb-only.gather.out"
    threads: 1
    shell: """
        sourmash gather {input} {GTDB_DB} -o {output.csv} \
           -k 31 --threshold-bp=0 --scaled 10_000 >& {output.txt}
    """
