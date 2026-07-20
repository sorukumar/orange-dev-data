path = '/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/scripts/lab/compare_knots.py'
with open(path, 'r') as f:
    code = f.read()

# Remove the faulty import at the top
code = code.replace("from scripts.utils.subsystem import score_with_details\n", "")

# Insert it after sys.path.append
target = "sys.path.append(str(PROJECT_ROOT))"
replacement = "sys.path.append(str(PROJECT_ROOT))\nfrom scripts.utils.subsystem import score_with_details"

if target in code:
    code = code.replace(target, replacement)
    with open(path, 'w') as f:
        f.write(code)
    print("Fixed import placement")
else:
    print("Could not find sys.path.append")
