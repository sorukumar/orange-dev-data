#!/bin/bash
exec > /tmp/run_weekly_interactive.log 2>&1

# Pop up a dialog asking the user if they want to run the pipeline
osascript <<EOF
tell application "System Events"
    activate
    set question to display dialog "Time for the weekly orange-dev-data refresh. Run the pipeline now?" buttons {"Cancel", "Run Now"} default button "Run Now" with title "Weekly Refresh"
end tell

if button returned of question is "Run Now" then
    tell application "Terminal"
        activate
        do script "cd /Users/saurabhkumar/Desktop/Work/github/orange-dev-data && echo '🚀 Starting Weekly Pipeline...' && /opt/anaconda3/bin/python3 scripts/rebuild_weekly.py && echo '\n\n=== Generating Date Report ===' && /opt/anaconda3/bin/python3 scripts/maintenance/check_dates.py; echo '\n✨ Pipeline and Report Complete! You can review the numbers above.'"
    end tell
end if
EOF
