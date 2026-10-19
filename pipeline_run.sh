#!/bin/bash
echo "Move to directory"
cd /home/frettie/nkcr_catmandu_pipeline;
#. /home/frettie/nkcr_catmandu_pipeline/bin/activate && python3 /home/frettie/nkcr_catmandu_pipeline/main.py --input /home/frettie/nkcr_catmandu_pipeline/output.csv ; deactivate
# the virtualenv is .venv on the notebook, venv on the Mac mini
VENV=/Users/jirisedlacek/htdocs/nkcr_catmandu/.venv
[ -d "$VENV" ] || VENV=/Users/jirisedlacek/htdocs/nkcr_catmandu/venv
. "$VENV/bin/activate" && python3 /Users/jirisedlacek/htdocs/nkcr_catmandu/main.py --input /Users/jirisedlacek/htdocs/nkcr_catmandu/output.csv ; deactivate

