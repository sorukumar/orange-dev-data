# Update compare_knots.py
path_py = '/Users/saurabhkumar/Desktop/Work/github/orange-dev-data/scripts/lab/compare_knots.py'
with open(path_py, 'r') as f:
    code = f.read()

target_py = """        if dev['provenance'] in ['ancient_ghost', 'ghost', 'fast_tracked']:
            # Earliest salvaged commits
            salvaged = [c for c in dev_commits if c['delta_days'] > 60]
            salvaged.sort(key=lambda c: c['author_date'])
            for c in salvaged[:3]:
                # Apply categorization
                cat, _, _ = score_with_details(c['subject'])
                c['category'] = cat
            dev['sample_commits'] = salvaged[:3]"""

repl_py = """        if dev['provenance'] in ['ancient_ghost', 'ghost', 'fast_tracked']:
            salvaged = [c for c in dev_commits if c['delta_days'] > 60]
            salvaged.sort(key=lambda c: c['author_date'])
            samples = []
            if len(salvaged) > 1:
                # Append oldest and newest
                samples = [salvaged[0], salvaged[-1]]
            elif len(salvaged) == 1:
                samples = salvaged
            
            for c in samples:
                # Apply categorization
                cat, _, _ = score_with_details(c['subject'])
                c['category'] = cat
            dev['sample_commits'] = samples"""

if target_py in code:
    code = code.replace(target_py, repl_py)
    with open(path_py, 'w') as f:
        f.write(code)
    print("Updated compare_knots.py")
else:
    print("Could not find target in compare_knots.py")


# Update script.js
path_js = '/Users/saurabhkumar/Desktop/Work/github/orange-dev-tracker/lab/knots/script.js'
with open(path_js, 'r') as f:
    js = f.read()

target_js = """commitsHtml = `<div style="margin-top: 10px; text-align: left; background: rgba(0,0,0,0.2); padding: 8px 8px 2px 8px; border-radius: 4px;"><div style="font-size: 0.7em; color: var(--text-secondary); margin-bottom: 6px; text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px;">Earliest Salvaged</div>${earliest}</div>`;"""
repl_js = """commitsHtml = `<div style="margin-top: 10px; text-align: left; background: rgba(0,0,0,0.2); padding: 8px 8px 2px 8px; border-radius: 4px;"><div style="font-size: 0.7em; color: var(--text-secondary); margin-bottom: 6px; text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px;">Oldest & Newest</div>${earliest}</div>`;"""

if target_js in js:
    js = js.replace(target_js, repl_js)
    with open(path_js, 'w') as f:
        f.write(js)
    print("Updated script.js")
else:
    print("Could not find target in script.js")

