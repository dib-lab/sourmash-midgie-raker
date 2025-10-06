# sourmash-midgie-raker

Utilities and workflows for examining (metagenome) binning results.

(A "midgie" is a trash heap, and a "midgie raker" is someone who combs
through trash heaps looking for good stuff.)

## The layout

midgie-raker is organized in a series of directories, each with their own
snakemake workflow.

Start with `sketch-raw`, which takes a bunch of FASTA files of MAGs
and sketches them with `sourmash scripts manysketch`.

Then, move on to `rename`, which takes the sketches, builds taxonomic
profiles of them with `gather` against GTDB, and renames them consistently.

*Next, run `decontam`, which profiles the sketches against eukaryotes + GTDB,
and flags bins that contain substantial amounts of multi-class content.

Then, run `min-set-cov`. This profiles the bins against GTDB,
determining the size of the minimum set cover for each bin, and also
calculating the novelty in each bin.

Finally, the `bin-coverage` code will calculate coverage histograms
for selected MAG x metagenome comparisons.
