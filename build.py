name: Update FEMA Data

on:
  schedule:
    - cron: '0 4 * * 0'  # Every Sunday at 4am UTC
  workflow_dispatch:       # Also lets you trigger it manually

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install pandas requests openpyxl
      - run: python build.py
      - name: Commit and push
        run: |
          git config user.name "github-actions"
          git config user.email "actions@github.com"
          git add index.html
          git diff --staged --quiet || git commit -m "Auto-update FEMA data $(date +%Y-%m-%d)"
          git push
