#!/bin/bash
echo "Move to directory"
cd /Users/jirisedlacek/htdocs/nkcr_catmandu;
rm aut.xml.gz
rm aut.xml
echo "Run Catmandu"
wget -q https://aleph.nkp.cz/data/aut.xml.gz
echo "Downloaded AUT XML GZ"
gzip -df aut.xml.gz
echo "Extracted AUT XML GZ"
catmandu convert MARC --type XML --fix biografickapole-pro-frettieho.fix to CSV --fields "_id,100a,100b,100d,100q,151a,046f,046g,370a,370b,370f,372a,374a,375a,377a,400ia,500ia7,0247a-isni,0247a-wikidata,0247a,0247a-orcid,678a" < aut.xml > output.csv
echo "Converted to CSV"
#cp output.csv /var/www/autority.wikimedia.cz/output.csv
# TODO: Copy output.csv to autority.wikimedia.cz by scp
#echo "Copied output.csv to autority.wikimedia.cz"
#rm cache.csv
. /Users/jirisedlacek/htdocs/nkcr_catmandu/venv/bin/activate && python3 /Users/jirisedlacek/htdocs/nkcr_catmandu/main.py --input /Users/jirisedlacek/htdocs/nkcr_catmandu/output.csv ; deactivate

