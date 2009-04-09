#BASENAME=uk-2007-05
BASENAME=eu-2005

for ext in .properties .graph.gz .offsets.gz .md5sums .urls.gz .mph .fcl.gz; do
    wget -c http://nexus.law.dsi.unimi.it/webdata/$BASENAME/$BASENAME$ext
done
md5sum -c $BASENAME.md5sums
gunzip $BASENAME.graph.gz
gunzip $BASENAME.offsets.gz
gunzip $BASENAME.urls.gz
gunzip $BASENAME.fcl.gz
md5sum -c $BASENAME.md5sums
