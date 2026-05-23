#!/bin/bash
set -e
echo "Starting quick rebuild..."
python3 scripts/02_process/reviews.py
python3 scripts/02_process/github_social.py
python3 scripts/02_process/core.py
python3 scripts/02_process/merge_social.py
python3 scripts/02_process/governance.py
python3 scripts/03_analyze/efficiency.py
python3 scripts/03_analyze/expertise.py
python3 scripts/03_analyze/influence.py
python3 scripts/02_process/unify_contributors.py
python3 scripts/04_deliver/registry.py
python3 scripts/04_deliver/ui_artifacts.py
echo "Quick rebuild complete!"
