test:
	snakemake -j 4 --configfile config-test.yaml -- rename

clean-test:
	snakemake -j 4 --configfile config-test.yaml --delete-all-output
