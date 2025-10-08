# sketch the raw bins with their original names
RAW_GENOME_LOCATION=config.get('base')['bin_location'].rstrip('/')
EXTENSION=config.get('base')['extension']
RAW_NAMES, = glob_wildcards(RAW_GENOME_LOCATION + '/' + '{name}' + '.' + EXTENSION)

print('using bin location:', RAW_GENOME_LOCATION)
print('using bin extension:', EXTENSION)
print(f'found {len(RAW_NAMES)} raw genomes.')
print(f'example name:', RAW_NAMES[0] + '.' + EXTENSION)

###

import csv

rule sketch_raw:
    input:
        OUTPUTS+'/raw/manysketch-raw.csv',
        OUTPUTS+'/raw/bin-sketches.sig.zip',

rule sketch_raw_make_manysketch_csv:
    output:
        OUTPUTS+'/raw/manysketch-raw.csv'
    run:
        with open(output[0], 'w', newline='') as fp:
            w = csv.writer(fp)
            w.writerow(['name', 'genome_filename', 'protein_filename'])
            for name in RAW_NAMES:
                location = f"{RAW_GENOME_LOCATION}/{name}.{EXTENSION}"
                w.writerow([name, location, ''])

rule sketch_raw_sketch:
    input:
        OUTPUTS+'/raw/manysketch-raw.csv',
    output:
        OUTPUTS+'/raw/bin-sketches.sig.zip',
    threads: 64
    params:
        sketchtype="dna,k=31"
    shell: """
        sourmash scripts manysketch -c {threads} {input} -o {output} \
            -p {params.sketchtype}
    """
