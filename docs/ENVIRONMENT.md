# Orange Dev Data: Python Environment Requirements

## 🐍 Mandatory Python Path
All scripts in this repository **must** be executed using the Anaconda Python 3 distribution located at:
`/opt/anaconda3/bin/python3`

## 📦 Required Dependencies
The pipeline relies on specific versions of the following libraries installed in the Anaconda base environment:
- `pandas` (with Parquet support)
- `pyarrow` or `fastparquet`
- `networkx` (for PageRank and Graph analytics)
- `requests` (for Delving and GitHub API calls)

## 🤖 Note for AI Assistants
When asked to "run the pipeline" or "execute a script" in this workspace, **never** use the default system `python3`. Always use the full absolute path to the Anaconda binary above to ensure access to the consolidated intelligence libraries.
