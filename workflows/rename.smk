# rename the raw bins into something sensible, if desired

RAW_GENOME_LOCATION=config.get('base')['bin_location'].rstrip('/')
EXTENSION=config.get('base')['extension']
NAMES, = glob_wildcards(RAW_GENOME_LOCATION + '/' + '{name}' + '.' + EXTENSION)

print('using bin location:', RAW_GENOME_LOCATION)
print('using bin extension:', EXTENSION)
print(f'found {len(NAMES)} raw genomes.')
print(f'example name:', NAMES[0] + '.' + EXTENSION)

SKETCHES=OUTPUTS+'/raw/bin-sketches.sig.zip'

KEEP_ORIGINAL_IDENT=config.get('rename')['keep_original_ident']
NEW_PREFIX=config.get('rename')['new_prefix']

GTDB_TK_OUTPUT = config.get('rename').get('gtdbtk_classify', [])

###

import csv
import sourmash

FILE_INFO = {}
ORIG_NAME = {}
for name in NAMES:
    ident = name.split(' ')[0]
    FILE_INFO[ident] = RAW_GENOME_LOCATION + '/' + name + '.' + EXTENSION
    ORIG_NAME[ident] = name


rule rename:
    input:
        OUTPUTS+'/rename/raw-sketch-gather.csv',
        OUTPUTS+'/rename/raw-sketch-gather.with-lineages.csv',
        OUTPUTS+'/rename/raw-sketch-gather.classifications.csv',
        OUTPUTS+'/rename/bin-sketches.lineages.csv',
        OUTPUTS+'/rename/raw-sketch.unclassified.csv',
        OUTPUTS+'/rename/manysketch-renamed.csv',
        OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        OUTPUTS+'/rename/bin-sketches.taxburst.html',

rule rename_custom_lineages_from_gtdbtk:
    params:
        gtdbtk_tsv=GTDB_TK_OUTPUT,
    output:
        OUTPUTS+'/rename/raw-sketch.custom-lineages.csv'
    shell: """
        ./scripts/process-gtdbtk.py {params.gtdbtk_tsv} --allow-empty -o {output}
    """


rule rename_gather_raw_sketches:
    input:
        SKETCHES
    output:
        OUTPUTS+'/rename/raw-sketch-gather.csv',
    threads: 1                  # against rocksdb, yah?
    shell: """
        sourmash scripts fastmultigather {input} {GTDB_DB} \
           -k 31 --threshold-bp=100_000 --scaled=10_000 \
           -o {output} -c {threads}
    """

rule rename_tax_annotate:
    input:
        OUTPUTS+'/rename/raw-sketch-gather.csv',
    output:
        OUTPUTS+'/rename/raw-sketch-gather.with-lineages.csv',
    params:
        dir=OUTPUTS+'/rename',
    shell: """
        sourmash tax annotate -g {input} -t {GTDB_TAX} -o {params.dir}
    """

rule rename_tax_genome:
    input:
        OUTPUTS+'/rename/raw-sketch-gather.csv',
    output:
        OUTPUTS+'/rename/raw-sketch-gather.classifications.csv'
    params:
        prefix=OUTPUTS+'/rename/raw-sketch-gather',
    shell: """
        sourmash tax genome -g {input} -t {GTDB_TAX} -o {params.prefix}
    """

rule rename_process_csv:
    input:
        classify=OUTPUTS+'/rename/raw-sketch-gather.classifications.csv',
        sketches=SKETCHES,
        custom=OUTPUTS+'/rename/raw-sketch.custom-lineages.csv',
    output:
        lineages=OUTPUTS+'/rename/bin-sketches.lineages.csv',
        unclass=OUTPUTS+'/rename/raw-sketch.unclassified.csv',
        rename=OUTPUTS+'/rename/manysketch-renamed.csv',
    run:
        classify_d = {}
        with open(input.classify, 'r', newline='') as fp:
            r = csv.DictReader(fp)
            for row in r:
                ident = row['query_name'].split(' ')[0]
                classify_d[ident] = row

        with open(input.custom, 'r', newline='') as fp:
            r = csv.DictReader(fp)
            custom_rows = list(r)

            for row in custom_rows:
                ident = row['ident']
                row_d = dict(query_name=ident, lineage=row['lineage'],
                             status='match')
                classify_d[ident] = row_d

        lin_fp = open(output.lineages, 'w', newline='')
        lin_w = csv.writer(lin_fp)
        lin_w.writerow(['ident'] + RANKS)

        unclass_fp = open(output.unclass, 'w', newline='')
        unclass_w = csv.writer(unclass_fp)
        unclass_w.writerow(['ident'])

        rename_fp = open(output.rename, 'w', newline='')        
        rename_w = csv.writer(rename_fp)
        rename_w.writerow(['name', 'genome_filename', 'protein_filename'])

        found = set()
        n_classify = 0
        n_unclass = 0

        for n, (ident, row) in enumerate(classify_d.items()):
            if ident not in FILE_INFO:
                print(f'WARNING: we appear to have information about more bins than we found!? specifically: {ident}')
                continue

            assert ident not in found, ident
            found.add(ident)

            # calculate new ident, if desired
            if KEEP_ORIGINAL_IDENT:
                new_ident = ident
            else:
                new_ident = f"{NEW_PREFIX}_MAG{n}"

            # can we write a species classification?
            classify_at_species = False
            if row['status'] == 'match':
                lin = row['lineage'].split(';')
                if len(lin) == len(RANKS):
                    classify_at_species = True
                    n_classify += 1

                    lin_w.writerow([new_ident] + lin)

                    new_name = f"{new_ident} {lin[-1]} ({ident})"

            # if not, that's just fiiiine.
            if not classify_at_species:
                new_name = f"{new_ident} unknown ({ident})"
                unclass_w.writerow([ident])
                n_unclass += 1

            # write out new name
            genomefile = FILE_INFO[ident]
            rename_w.writerow([new_name, genomefile, ''])

        # check/confirm
        db = sourmash.load_file_as_index(input.sketches)
        mf = db.manifest

        # one source of unclassified is no species match; another is no
        # gather match!
        # CTB: clean up/refactor so this is a single loop :(
        for row in mf.rows:
            ident = row['name'].split(' ')[0]
            assert ident, f"'{ident}' is empty for {row['name']}"
            if ident not in found:
                found.add(ident)

                # calculate new ident, if desired
                n += 1
                if KEEP_ORIGINAL_IDENT:
                    new_ident = ident
                else:
                    new_ident = f"{NEW_PREFIX}_MAG{n}"

                new_name = f"{new_ident} unknown ({ident})"
                genomefile = FILE_INFO[ident]
                rename_w.writerow([new_name, genomefile, ''])
                
                unclass_w.writerow([ident])
                n_unclass += 1

        print(f'n_unclass: {n_unclass}')
        print(f'n_classify: {n_classify}')
        print(f'found: {len(found)}')
        print(f'names: {len(NAMES)}')

        assert n_unclass + n_classify == len(NAMES), "are we missing something? (msg 1)?"
        assert len(found) == len(NAMES), "are we missing something (msg 2)?"

        rename_fp.close()
        lin_fp.close()
        unclass_fp.close()

rule rename_sketch:
    input:
        OUTPUTS+'/rename/manysketch-renamed.csv',
    output:
        OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
    threads: 64
    params:
        sketchtype="dna,k=21,k=31,k=51"
    shell: """
        sourmash scripts manysketch -c {threads} {input} -o {output} \
            -p {params.sketchtype}
    """

rule rename_taxburst_genomes:
    input:
        sketches=OUTPUTS+'/rename/bin-sketches.renamed.sig.zip',
        tax=OUTPUTS+'/rename/bin-sketches.lineages.csv',
    output:
        html=OUTPUTS+'/rename/bin-sketches.taxburst.html',
        json=OUTPUTS+'/rename/bin-sketches.taxburst.json',
    shell: """
        ./scripts/build-sourmash-db-view.py {input.sketches} -t {input.tax} \
           --max-rank-num=7 -o {output.html} --save-json {output.json}
    """
