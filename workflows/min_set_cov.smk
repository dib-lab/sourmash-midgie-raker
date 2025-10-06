# Run a minimum set cover on things
#
# TODO:
# - display tax output

SKETCHES = OUTPUTS+'/rename/bin-sketches.renamed.sig.zip'

import csv
import collections
import os


rule min_set_cov:
    input:
        OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.csv',
        OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d',
        OUTPUTS+'/min-set-cov/.get-matches.touch',
        OUTPUTS+'/min-set-cov/.gather-ann.touch'


rule min_set_cov_fastmultigather:
    input:
        SKETCHES
    output:
        protected(OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.csv')
    threads: 1
    shell: """
        sourmash scripts fastmultigather -k 31 {input} {GTDB_DB} -o {output} \
            --threshold-bp=0 -c 1
    """

rule min_set_cov_gtdb_mf:
    input:
        GTDB_DB,
    output:
        OUTPUTS+'/gtdb.mf.sqldb',
    shell: """
        sourmash sig collect {input} -o {output} -F sql -k 31
    """

checkpoint min_set_cov_split_results:
    input:
        OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.csv'
    output:
        directory(OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d')
    run:
        os.mkdir(output[0])

        # collect rows by query_name
        results = collections.defaultdict(list)
        fieldnames = None
        with open(input[0], 'r', newline='') as fp:
            r = csv.DictReader(fp)
            
            for row in r:
                results[row['query_name']].append(row)
            fieldnames = r.fieldnames

        print(f'loaded gather results for {len(results)} queries')

        # split out into different CSVs
        for query_name, rows in results.items():
            ident = query_name.split(' ')[0]
            outpath = output[0] + '/' f'{ident}.gather.csv'
            with open(outpath, 'w', newline='') as fp:
                w = csv.DictWriter(fp, fieldnames=fieldnames)
                w.writeheader()
                for row in rows:
                    w.writerow(row)

def min_set_cov_aggregate_gather_files_mf(wc):
    checkpoint_output = checkpoints.min_set_cov_split_results.get(**wc).output[0]

    idents = glob_wildcards(OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{x}.gather.csv').x
    return expand(OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{ident}.gtdb-matches.mf.sqlite',
                  ident=idents)


def min_set_cov_aggregate_gather_files_ann(wc):
    checkpoint_output = checkpoints.min_set_cov_split_results.get(**wc).output[0]

    idents = glob_wildcards(OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{x}.gather.csv').x
    return expand(OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{ident}.gather.with-lineages.csv',
                  ident=idents)


rule min_set_cov_process_gather_csvs:
    input:
        min_set_cov_aggregate_gather_files_mf
    output:
        touch(OUTPUTS+'/min-set-cov/.get-matches.touch')


rule min_set_cov_process_gather_csvs2:
    input:
        min_set_cov_aggregate_gather_files_ann
    output:
        touch(OUTPUTS+'/min-set-cov/.gather-ann.touch')


rule process_gather_csvs_wc:
    input:
        picklist=OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{ident}.gather.csv',
        manifest=OUTPUTS+'/gtdb.mf.sqldb',
    output:
        OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{ident}.gtdb-matches.mf.sqlite',
    shell: """
        sourmash sig check --picklist {input.picklist}:match_name:ident \
           {input.manifest} -m {output} -F sql --abspath
    """

rule process_gather_csvs2_wc:
    input:
        OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{ident}.gather.csv',
    output:
        OUTPUTS+'/min-set-cov/sketches.x.gtdb.fastgather.d/{ident}.gather.with-lineages.csv',
    shell: """
        sourmash tax annotate -g {input} -t {GTDB_TAX} \
           -o sketches.x.gtdb.fastgather.d
    """
