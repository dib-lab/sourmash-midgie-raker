rule host_screen:
    input:
        OUTPUTS+'/host/gtdb.x.host.manysearch.csv',
        OUTPUTS+'/host/bins.x.host.manysearch.csv',
        OUTPUTS+'/host/picklist-exclude.csv',
        OUTPUTS+'/host/clean-gtdb-rs226.dna.sig.zip',
        OUTPUTS+'/host/clean-bins.dna.sig.zip',
        OUTPUTS+"/host/clean-bins.x.host.manysearch.csv",
        OUTPUTS+"/host/clean-gtdb.x.host.manysearch.csv",
        OUTPUTS+'/host/clean-gtdb+bins.species.k21.sig.zip',
        OUTPUTS+'/host/clean-gtdb+bins.species.k31.sig.zip',
        OUTPUTS+'/host/clean-gtdb+bins.species.k31.rocksdb',
        OUTPUTS+'/host/clean-gtdb.species.k21.sig.zip',
        OUTPUTS+'/host/clean-gtdb.species.k31.sig.zip',
        OUTPUTS+'/host/clean-gtdb.species.k31.rocksdb',

rule arty_tax:
    input:
        lin='/group/ctbrowngrp5/sourmash-db/genbank-euks-2025.01/eukaryotes.lineages.csv',
        sketches='/group/ctbrowngrp5/sourmash-db/genbank-euks-2025.01/vertebrates.k51.sig.zip',
    output:
        tax="arty.tax.csv",
        mf="arty.mf.csv",
    shell: """
        sourmash tax grep Artiodactyla -t {input.lin} -o {output.tax}
        sourmash sig check --picklist {output.tax}:ident:ident \
           {input.sketches} -m {output.mf} --abspath
    """

rule manysearch_host_bins:
    input:
        bins=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        mf="arty.mf.csv",
    output:
        OUTPUTS+"/host/bins.x.host.manysearch.csv",
    threads: 64
    shell: """
        sourmash scripts manysearch -k 51 -s 10_000 -t 0 -c {threads} \
            {input.bins:q} {input.mf:q} -o {output:q}
    """

rule manysearch_host_bins_clean:
    input:
        bins=OUTPUTS+'/host/clean-bins.dna.sig.zip',
        mf="arty.mf.csv",
    output:
        OUTPUTS+"/host/clean-bins.x.host.manysearch.csv",
    threads: 64
    shell: """
        sourmash scripts manysearch -k 51 -s 10_000 -t 0 -c {threads} \
            {input.bins:q} {input.mf:q} -o {output:q}
    """

rule manysearch_host_gtdb:
    input:
        gtdb=GTDB_K51_ZIP,
        mf="arty.mf.csv",
    output:
        OUTPUTS+"/host/gtdb.x.host.manysearch.csv",
    threads: 64
    shell: """
        sourmash scripts manysearch -k 51 -s 10_000 -t 0 -c {threads} \
            {input.gtdb:q} {input.mf:q} -o {output:q}
    """

rule manysearch_host_gtdb_clean:
    input:
        gtdb=OUTPUTS+'/host/clean-gtdb-rs226.dna.sig.zip',
        mf="arty.mf.csv",
    output:
        OUTPUTS+"/host/clean-gtdb.x.host.manysearch.csv",
    threads: 64
    shell: """
        sourmash scripts manysearch -k 51 -s 10_000 -t 0 -c {threads} \
            {input.gtdb:q} {input.mf:q} -o {output:q}
    """

rule make_picklist:
    input:
        OUTPUTS+'/host/gtdb.x.host.manysearch.csv',
        OUTPUTS+'/host/bins.x.host.manysearch.csv',
    output:
        OUTPUTS+'/host/picklist-exclude.csv',
    shell: """
        scripts/manysearch-to-picklist.py {input} -o {output}
    """

rule clean_bins:
    input:
        bins=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        pl=OUTPUTS+'/host/picklist-exclude.csv',
    output:
        OUTPUTS+'/host/clean-bins.dna.sig.zip',
    shell: """
        sourmash sig cat --picklist {input.pl}:ident:ident:exclude \
           {input.bins} -o {output}
    """

rule clean_gtdb:
    input:
        gtdb=[GTDB_ZIP, GTDB_K21_ZIP, GTDB_K51_ZIP],
        pl=OUTPUTS+'/host/picklist-exclude.csv',
    output:
        OUTPUTS+'/host/clean-gtdb-rs226.dna.sig.zip',
    shell: """
        sourmash sig cat --picklist {input.pl}:ident:ident:exclude \
           {input.gtdb} -o {output}
    """

rule host_make_species_pangenome_bins:
    input:
        gtdb=OUTPUTS+'/host/clean-gtdb-rs226.dna.sig.zip',
        bins=OUTPUTS+'/host/clean-bins.dna.sig.zip',
        tax=[OUTPUTS+'/rename/bin-sketches.lineages.csv',
             GTDB_TAX],
    output:
        OUTPUTS+'/host/clean-gtdb+bins.species.k{k}.sig.zip',
    shell: """
        sourmash scripts pangenome_createdb {input.bins} {input.gtdb} -t {input.tax} \
           -k {wildcards.k} -o {output} --allow-missing
    """
    
rule host_make_species_pangenome_gtdb:
    input:
        gtdb=OUTPUTS+'/host/clean-gtdb-rs226.dna.sig.zip',
        tax=GTDB_TAX,
    output:
        OUTPUTS+'/host/clean-gtdb.species.k{k}.sig.zip',
    shell: """
        sourmash scripts pangenome_createdb {input.gtdb} -t {input.tax} \
           -k {wildcards.k} -o {output} --allow-missing
    """

rule host_make_rocksdb:
    input:
        OUTPUTS+'/host/{filename}.k{k}.sig.zip',
    output:
        OUTPUTS+'/host/{filename}.k{k}.rocksdb',
    shell: """
        sourmash index -F rocksdb -k {wildcards.k} {output} {input}
    """
