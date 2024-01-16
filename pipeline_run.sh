#!/bin/bash
echo "Move to directory"
cd /home/frettie/nkcr_catmandu_pipeline;
. /home/frettie/nkcr_catmandu_pipeline/bin/activate && python3 /home/frettie/nkcr_catmandu_pipeline/main.py --input /home/frettie/nkcr_catmandu_pipeline/output.csv ; deactivate >> /home/frettie/nkcr_catmandu_pipeline.log 2>&1

