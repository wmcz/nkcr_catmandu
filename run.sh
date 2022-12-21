#!/bin/bash
echo "Move to directory"
cd /home/frettie/nkcr_catmandu_pipeline;
rm aut.xml.gz
rm aut.xml
echo "Run Catmandu"
wget https://aleph.nkp.cz/data/aut.xml.gz
echo "Downloaded AUT XML GZ"
gzip -df aut.xml.gz
echo "Extracted AUT XML GZ"
/usr/local/bin/catmandu convert MARC --type XML --fix biografickapole-pro-frettieho.fix to CSV --fields "_id,100a,100b,100d,100q,046f,046g,370a,370b,370f,374a,375a,377a,400ia,500ia7,678a,0247a-isni,0247a-wikidata,0247a,0247a-orcid" < aut.xml > output.csv
echo "Converted to CSV"
cp output.csv /var/www/autority.wikimedia.cz/output.csv
echo "Copied output.csv to autorita.wikimedia.cz"
#rm cache.csv
. /home/frettie/nkcr_catmandu_pipeline/bin/activate && python3 /home/frettie/nkcr_catmandu_pipeline/main.py --input /home/frettie/nkcr_catmandu_pipeline/output.csv ; deactivate

