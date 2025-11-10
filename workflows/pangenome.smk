EXPLAIN_METAGS_BIG, = glob_wildcards('/home/ctbrown/scratch3/2025-other-pig-bins/annie-dl/sketches_2000/6-1a-smash/{metag}.sig.gz')

EXTRACT_SPECIES = {'Escherichia_coli': 'GCF_946909825 s__Escherichia coli',
                    'Lactobacillus_amylovorus': 'GCA_004552585 s__Lactobacillus amylovorus',
                    'Cryptobacteroides_sp900546925': 'GCA_037297645 s__Cryptobacteroides sp900546925',
                    'UBA2868_sp004552595': 'GCA_945229155 s__UBA2868 sp004552595',
                    'JAFBIX01_sp021531895': 'GCA_945876855 s__JAFBIX01 sp021531895',
                    'Mogibacterium_A_kristiansenii': 'GCA_036550655 s__Mogibacterium_A kristiansenii',
                    'Sodaliphilus_sp004557565': 'GCA_022779425 s__Sodaliphilus sp004557565',
                    'Bariatricus_sp004560705': 'GCA_029378305 s__Bariatricus sp004560705',
                    'Prevotella_sp002251295': 'GCA_034114805 s__Prevotella sp002251295',
                    'Holdemanella_porci': 'GCA_945912825 s__Holdemanella porci',
                    'Floccifex_porci': 'GCA_016297875 s__Floccifex porci',
                    'JALFVM01_sp022787145': 'GCA_034115705 s__JALFVM01 sp022787145'}

rule pangenomedb:
    input:
        OUTPUTS+'/rename/gtdb+bins.species.sig.zip',
        OUTPUTS+'/rename/gtdb+bins.species.k21.sig.zip',
        OUTPUTS+'/rename/gtdb+bins.species.k31.rocksdb',
        SHARED+'/gtdb.species.sig.zip',
        SHARED+'/gtdb.species.k21.sig.zip',
        SHARED+'/gtdb.species.k31.rocksdb',
        SHARED+'/gtdb-merged.species.sig.zip',
        expand(OUTPUTS+'/pangenome/{metag}.gtdb+bins.species.gather.csv',
               metag=EXPLAIN_METAGS),
        expand(OUTPUTS+'/pangenome/{metag}.gtdb.species.gather.csv',
               metag=EXPLAIN_METAGS),
        expand(OUTPUTS+'/pangenome-expbig/{metag}.gtdb+bins.species.gather.csv',
               metag=EXPLAIN_METAGS_BIG),
        expand(OUTPUTS+'/pangenome-expbig/{metag}.gtdb.species.gather.csv',
               metag=EXPLAIN_METAGS_BIG),
        expand(OUTPUTS+'/core/{v}.sig.zip', v=EXTRACT_SPECIES),
        expand(OUTPUTS+'/core_isect/{v}.x.{metag}.sig.zip',
               v=EXTRACT_SPECIES, metag=EXPLAIN_METAGS),
        expand(OUTPUTS+'/core_isect/{v}.x.{metag}.abundhist.png',
               v=EXTRACT_SPECIES, metag=EXPLAIN_METAGS),

rule gtdb_bins_mf:
    input:
        bins=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        gtdb=GTDB_ZIP,
    output:
        OUTPUTS+'/rename/gtdb+bins.mf.csv',
    shell: """
        sourmash sig collect -F csv -o {output} {input} --abspath
    """

rule gtdb_bins_mf_k21:
    input:
        bins=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        gtdb=GTDB_K21_ZIP,
    output:
        OUTPUTS+'/rename/gtdb+bins.k21.mf.csv',
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
        
rule gtdb_bins_species_pangenome_k21:
    input:
        mf=OUTPUTS+'/rename/gtdb+bins.k21.mf.csv',
        tax=[OUTPUTS+'/rename/bin-sketches.lineages.csv',
             GTDB_TAX],
    output:
        OUTPUTS+'/rename/gtdb+bins.species.k21.sig.zip',
    shell: """
        sourmash scripts pangenome_createdb {input.mf} -t {input.tax} \
           -k 21 -o {output}
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

rule gtdb_species_pangenome_k21:
    input:
        zip=GTDB_K21_ZIP,
        tax=GTDB_TAX,
    output:
        SHARED+'/gtdb.species.k21.sig.zip',
    shell: """
        sourmash scripts pangenome_createdb {input.zip} -t {input.tax} \
           -k 21 -o {output}
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

rule gtdb_bins_species_gather_big:
    input:
        metag='/home/ctbrown/scratch3/2025-other-pig-bins/annie-dl/sketches_2000/6-1a-smash/{metag}.sig.gz',
        db=OUTPUTS+'/rename/gtdb+bins.species.k31.rocksdb',
    output:
        csv=touch(OUTPUTS+'/pangenome-expbig/{metag}.gtdb+bins.species.gather.csv'),
        out=touch(OUTPUTS+'/pangenome-expbig/{metag}.gtdb+bins.species.gather.out'),
    shell: """
        sourmash gather -k 31 --scaled 10_000 --threshold-bp=0 \
            {input.metag} {input.db} -o {output.csv} > {output.out} || true
    """

rule gtdb_species_gather_big:
    input:
        metag='/home/ctbrown/scratch3/2025-other-pig-bins/annie-dl/sketches_2000/6-1a-smash/{metag}.sig.gz',
        db=SHARED+'/gtdb.species.k31.rocksdb',
    output:
        csv=touch(OUTPUTS+'/pangenome-expbig/{metag}.gtdb.species.gather.csv'),
        out=touch(OUTPUTS+'/pangenome-expbig/{metag}.gtdb.species.gather.out')
    shell: """
        sourmash gather -k 31 --scaled 10_000 --threshold-bp=0 \
            {input.metag} {input.db} -o {output.csv} > {output.out} || true
    """

rule get_core_sig:
    input:
        OUTPUTS+'/rename/gtdb+bins.species.sig.zip',
    output:
        OUTPUTS+'/core/{name}.sig.zip'
    params:
        ident = lambda w: EXTRACT_SPECIES[w.name]
    shell: """
        sourmash sig grep {params.ident:q} {input} -o {output}
    """

rule isect_core:
    input:
        species=OUTPUTS+'/core/{v}.sig.zip',
        metag='inputs/metags/{metag}.sig',
    output:
        OUTPUTS+'/core_isect/{v}.x.{metag}.sig.zip',
    shell: """
        sourmash sig intersect -A {input.metag} {input.metag} {input.species} \
           -o {output} -k 31
    """

rule isect_core_abundhist:
    input:
        OUTPUTS+'/core_isect/{name}.sig.zip',
    output:
        OUTPUTS+'/core_isect/{name}.abundhist.png',
    shell: """
        sourmash scripts abundhist {input} --figure {output} --bins 100 --figure-title {name} -k 31 --silent
    """
