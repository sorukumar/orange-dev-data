path = '/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/scripts/lab/compare_knots.py'
with open(path, 'r') as f:
    code = f.read()

# Add import at the top if not exists
import_stmt = "from scripts.utils.subsystem import score_with_details"
if import_stmt not in code:
    code = code.replace("from datetime import datetime", f"from datetime import datetime\n{import_stmt}")

# In `both_sorted` processing, classify the sample_commits
target = """            salvaged.sort(key=lambda c: c['author_date'])
            dev['sample_commits'] = salvaged[:3]"""

replacement = """            salvaged.sort(key=lambda c: c['author_date'])
            for c in salvaged[:3]:
                # Apply categorization
                cat, _, _ = score_with_details(c['subject'])
                c['category'] = cat
            dev['sample_commits'] = salvaged[:3]"""

if target in code:
    code = code.replace(target, replacement)
    with open(path, 'w') as f:
        f.write(code)
    print("Updated compare_knots.py with categorization logic")
else:
    print("Could not find target in compare_knots.py")
