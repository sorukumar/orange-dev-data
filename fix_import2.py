path = '/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/scripts/lab/compare_knots.py'
with open(path, 'r') as f:
    code = f.read()

code = code.replace("from scripts.utils.subsystem import score_with_details\n", "")
target = "sys.path.insert(0, str(PROJECT_ROOT))"
replacement = "sys.path.insert(0, str(PROJECT_ROOT))\nfrom scripts.utils.subsystem import score_with_details"

code = code.replace(target, replacement)
with open(path, 'w') as f:
    f.write(code)
print("Fixed import placement")
