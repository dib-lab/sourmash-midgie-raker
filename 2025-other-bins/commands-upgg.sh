N=all4
sourmash sig collect -F csv ../outputs.{ath,upgg,ku,semibin2,single,ucd,vamb}/rename/bin-sketches.renamed.sig.zip -o ${N}.bins.mf.csv --abspath

/usr/bin/time -v sourmash scripts pairwise ${N}.bins.mf.csv \
              -o ${N}.bins.multisearch.csv -c 64 -k 31 --ani

sourmash scripts cluster --similarity average_containment_ani \
         ${N}.bins.multisearch.csv -o ${N}.bins.cluster.99.csv -t 0.99

sourmash scripts cluster --similarity average_containment_ani \
         ${N}.bins.multisearch.csv -o ${N}.bins.cluster.95.csv -t 0.95
