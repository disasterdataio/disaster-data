"""
build.py
Fetches live data from the FEMA OpenFEMA API, processes it,
and writes a self-contained index.html dashboard.

Run locally:  python build.py
Run in CI:    same command — GitHub Actions uses this directly.
"""

import json
import time
import datetime
import urllib.request
import urllib.parse
from collections import defaultdict

# ── CONFIG ────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.fema.gov/api/open/v2"
START_YEAR = 2000          # filter records from this fiscal year forward
PAGE_SIZE  = 10000         # max the API allows per call
SLEEP_SEC  = 0.5           # polite pause between paginated requests
TODAY      = datetime.date.today().isoformat()


# ═════════════════════════════════════════════════════════════════════════
# 1. FETCH FROM API
# ═════════════════════════════════════════════════════════════════════════

def fetch_all(endpoint, extra_filter="", fields=None):
    """Page through an OpenFEMA endpoint and return all records."""
    records = []
    skip    = 0
    total   = None
    base_filter = f"fyDeclared ge {START_YEAR}" if "Declaration" in endpoint else f"declarationRequestDate ge '{START_YEAR}-01-01T00:00:00.000Z'"

    filt = base_filter
    if extra_filter:
        filt = f"{base_filter} and {extra_filter}"

    select_param = ""
    if fields:
        select_param = "&$select=" + ",".join(fields)

    print(f"  Fetching {endpoint}...")

    while True:
        params = (
            f"?$top={PAGE_SIZE}"
            f"&$skip={skip}"
            f"&$filter={urllib.parse.quote(filt)}"
            f"&$inlinecount=allpages"
            f"&$orderby=id asc"
            + select_param
        )
        url = f"{BASE_URL}/{endpoint}{params}"

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "UWSWVA-FEMA-Explorer/1.0"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read())
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"    Retry {attempt+1} after error: {e}")
                time.sleep(3)

        batch = data.get(endpoint, [])
        records.extend(batch)

        if total is None:
            total = int(data.get("metadata", {}).get("count", 0))
            print(f"    Total records: {total}")

        skip += len(batch)
        print(f"    Fetched {skip}/{total}")

        if not batch or (total and skip >= total):
            break
        time.sleep(SLEEP_SEC)

    return records


# Declarations — only fields we need
DEC_FIELDS = [
    "femaDeclarationString", "disasterNumber", "state", "declarationType",
    "declarationDate", "fyDeclared", "incidentType", "declarationTitle",
    "incidentBeginDate", "designatedArea", "region", "id"
]

# Denials — only fields we need
DEN_FIELDS = [
    "declarationRequestNumber", "stateAbbreviation", "state",
    "declarationRequestType", "incidentName", "requestedIncidentTypes",
    "declarationRequestDate", "requestStatusDate", "currentRequestStatus",
    "region", "id"
]

print("Fetching declarations...")
raw_dec = fetch_all("DisasterDeclarationsSummaries", fields=DEC_FIELDS)
print(f"  → {len(raw_dec)} declaration records\n")

print("Fetching denials...")
raw_den = fetch_all("DeclarationDenials", extra_filter="currentRequestStatus eq 'Turndown'", fields=DEN_FIELDS)
print(f"  → {len(raw_den)} denial records\n")


# ═════════════════════════════════════════════════════════════════════════
# 2. PROCESS DATA
# ═════════════════════════════════════════════════════════════════════════

def parse_date(s):
    if not s:
        return None
    try:
        return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        return None

def days_between(d1, d2):
    if d1 and d2:
        delta = (d2 - d1).days
        return delta if delta >= 0 else None
    return None

def safe_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


print("Processing declarations...")
dec_processed = []
for r in raw_dec:
    fy = safe_int(r.get("fyDeclared"))
    if not fy or fy < START_YEAR or fy > 2026:
        continue
    dec_date    = parse_date(r.get("declarationDate"))
    begin_date  = parse_date(r.get("incidentBeginDate"))
    days_app    = days_between(begin_date, dec_date)
    dec_processed.append({
        "femaDeclarationString": r.get("femaDeclarationString", ""),
        "state":                 r.get("state", ""),
        "declarationType":       r.get("declarationType", ""),
        "declarationDate":       dec_date.isoformat() if dec_date else "",
        "fyDeclared":            fy,
        "incidentType":          r.get("incidentType", ""),
        "declarationTitle":      r.get("declarationTitle", ""),
        "designatedArea":        r.get("designatedArea", ""),
        "region":                r.get("region"),
        "days_to_approve":       days_app if days_app is not None else -1,
    })

print(f"  → {len(dec_processed)} processed\n")

print("Processing denials...")
den_processed = []
for r in raw_den:
    req_date = parse_date(r.get("declarationRequestDate"))
    sta_date = parse_date(r.get("requestStatusDate"))
    days_den = days_between(req_date, sta_date)
    state_ab = (r.get("stateAbbreviation") or "").strip()
    den_processed.append({
        "declarationRequestNumber": str(r.get("declarationRequestNumber", "")),
        "stateAbbreviation":        state_ab,
        "declarationRequestType":   r.get("declarationRequestType", ""),
        "requestedIncidentTypes":   r.get("requestedIncidentTypes", ""),
        "declarationRequestDate":   req_date.isoformat() if req_date else "",
        "requestStatusDate":        sta_date.isoformat() if sta_date else "",
        "currentRequestStatus":     r.get("currentRequestStatus", ""),
        "region":                   r.get("region"),
        "days_to_deny":             days_den if days_den is not None else -1,
    })

print(f"  → {len(den_processed)} processed\n")


# ═════════════════════════════════════════════════════════════════════════
# 3. AGGREGATE SUMMARY DATA
# ═════════════════════════════════════════════════════════════════════════

print("Building aggregates...")

# Filter valid processing times
dec_valid = [r for r in dec_processed if r["days_to_approve"] >= 0]
den_valid = [r for r in den_processed if r["days_to_deny"] >= 0]

def avg(lst):
    return round(sum(lst) / len(lst), 1) if lst else 0

# Year-over-year
yoy_dec = defaultdict(lambda: {"declarations": 0, "days": []})
for r in dec_valid:
    fy = r["fyDeclared"]
    if fy <= 2025:
        yoy_dec[fy]["declarations"] += 1
        yoy_dec[fy]["days"].append(r["days_to_approve"])

yoy_den = defaultdict(lambda: {"denials": 0, "days": []})
for r in den_valid:
    yr = int(r["declarationRequestDate"][:4]) if r["declarationRequestDate"] else 0
    if 2000 <= yr <= 2025:
        yoy_den[yr]["denials"] += 1
        yoy_den[yr]["days"].append(r["days_to_deny"])

all_years = sorted(set(list(yoy_dec.keys()) + list(yoy_den.keys())))
yoy = []
for yr in all_years:
    d  = yoy_dec.get(yr, {})
    dn = yoy_den.get(yr, {})
    yoy.append({
        "fyDeclared":    yr,
        "declarations":  d.get("declarations", 0),
        "avg_days":      avg(d.get("days", [])),
        "denials":       dn.get("denials", 0),
        "avg_days_deny": avg(dn.get("days", [])),
    })

# By incident type
inc_map = defaultdict(lambda: {"count": 0, "days": []})
for r in dec_valid:
    it = r["incidentType"] or "Unknown"
    inc_map[it]["count"] += 1
    inc_map[it]["days"].append(r["days_to_approve"])
by_incident = sorted(
    [{"incidentType": k, "count": v["count"], "avg_days": avg(v["days"])} for k, v in inc_map.items()],
    key=lambda x: -x["count"]
)

# By state
state_map = defaultdict(lambda: {"count": 0, "days": []})
for r in dec_valid:
    state_map[r["state"]]["count"] += 1
    state_map[r["state"]]["days"].append(r["days_to_approve"])
by_state = sorted(
    [{"state": k, "count": v["count"], "avg_days": avg(v["days"])} for k, v in state_map.items()],
    key=lambda x: -x["count"]
)

# By declaration type
dec_type_map = defaultdict(lambda: {"count": 0, "days": []})
for r in dec_valid:
    dec_type_map[r["declarationType"]]["count"] += 1
    dec_type_map[r["declarationType"]]["days"].append(r["days_to_approve"])
by_dec_type = [{"declarationType": k, "count": v["count"], "avg_days": avg(v["days"])} for k, v in dec_type_map.items()]

# Denials by type
den_inc_map = defaultdict(int)
for r in den_valid:
    den_inc_map[r["requestedIncidentTypes"] or "Unknown"] += 1
denials_by_type = sorted([{"requestedIncidentTypes": k, "count": v} for k, v in den_inc_map.items()], key=lambda x: -x["count"])

# Denials by state
den_state_map = defaultdict(lambda: {"count": 0, "days": []})
for r in den_valid:
    st = r["stateAbbreviation"]
    den_state_map[st]["count"] += 1
    den_state_map[st]["days"].append(r["days_to_deny"])
denials_by_state = sorted(
    [{"stateAbbreviation": k, "count": v["count"], "avg_days": avg(v["days"])} for k, v in den_state_map.items()],
    key=lambda x: -x["count"]
)

summary = {
    "yoy":            yoy,
    "byIncidentType": by_incident,
    "byState":        by_state,
    "byDecType":      by_dec_type,
    "denialsByType":  denials_by_type,
    "denialsByState": denials_by_state,
    "lastUpdated":    TODAY,
}

# ── State-level aggregates ────────────────────────────────────────────────
swva = ['Bland','Buchanan','Carroll','Craig','Dickenson','Floyd','Giles',
        'Grayson','Henry','Highland','Lee','Montgomery','Patrick','Pulaski',
        'Russell','Scott','Smyth','Tazewell','Washington','Wise','Wythe',
        'Bristol','Galax','Norton','Radford']

state_dec_map  = defaultdict(lambda: {"declarations": 0, "days": [], "incidents": defaultdict(int), "top_incident": ""})
for r in dec_valid:
    st = r["state"]
    if r["fyDeclared"] <= 2025:
        state_dec_map[st]["declarations"] += 1
        state_dec_map[st]["days"].append(r["days_to_approve"])
        state_dec_map[st]["incidents"][r["incidentType"] or "Unknown"] += 1

state_den_map = defaultdict(lambda: {"denials": 0, "days": []})
for r in den_valid:
    yr = int(r["declarationRequestDate"][:4]) if r["declarationRequestDate"] else 0
    if yr <= 2025:
        st = r["stateAbbreviation"]
        state_den_map[st]["denials"] += 1
        state_den_map[st]["days"].append(r["days_to_deny"])

state_summary = []
for st, d in state_dec_map.items():
    dn       = state_den_map.get(st, {})
    decl     = d["declarations"]
    denials  = dn.get("denials", 0)
    total_r  = decl + denials
    top_inc  = max(d["incidents"], key=d["incidents"].get) if d["incidents"] else ""
    state_summary.append({
        "state":         st,
        "declarations":  decl,
        "denials":       denials,
        "total_requests": total_r,
        "denial_rate":   round(denials / total_r * 100, 2) if total_r else 0,
        "avg_days":      avg(d["days"]),
        "avg_deny_days": avg(dn.get("days", [])),
        "top_incident":  top_inc,
    })

# State YoY
state_yoy_map = defaultdict(list)
for r in dec_valid:
    if r["fyDeclared"] <= 2025:
        state_yoy_map[r["state"]].append(r["fyDeclared"])

state_yoy = {}
for st, years_list in state_yoy_map.items():
    from collections import Counter
    yr_counts = Counter(years_list)
    state_yoy[st] = [{"y": yr, "c": cnt} for yr, cnt in sorted(yr_counts.items())]

# State incident breakdown
state_inc_map2 = defaultdict(lambda: defaultdict(int))
for r in dec_valid:
    if r["fyDeclared"] <= 2025:
        state_inc_map2[r["state"]][r["incidentType"] or "Unknown"] += 1

state_inc = {
    st: sorted([{"t": inc, "c": cnt} for inc, cnt in incs.items()], key=lambda x: -x["c"])
    for st, incs in state_inc_map2.items()
}

# State disaster list (unique per femaDeclarationString)
state_disasters = defaultdict(list)
seen = set()
for r in sorted(dec_valid, key=lambda x: x["declarationDate"], reverse=True):
    if r["fyDeclared"] > 2025:
        continue
    key = r["femaDeclarationString"]
    if key in seen:
        continue
    seen.add(key)
    state_disasters[r["state"]].append({
        "id":    r["femaDeclarationString"],
        "dt":    r["declarationType"],
        "date":  r["declarationDate"],
        "fy":    r["fyDeclared"],
        "inc":   r["incidentType"],
        "title": r["declarationTitle"],
        "days":  r["days_to_approve"],
        "reg":   r["region"],
    })

# Browse list (unique disasters, national)
browse = []
seen2 = set()
for r in sorted(dec_valid, key=lambda x: x["declarationDate"], reverse=True):
    if r["fyDeclared"] > 2025:
        continue
    key = r["femaDeclarationString"]
    if key in seen2:
        continue
    seen2.add(key)
    browse.append({
        "femaDeclarationString": r["femaDeclarationString"],
        "state":                 r["state"],
        "declarationType":       r["declarationType"],
        "declarationDate":       r["declarationDate"],
        "fyDeclared":            r["fyDeclared"],
        "incidentType":          r["incidentType"],
        "declarationTitle":      r["declarationTitle"],
        "region":                r["region"],
        "days_to_approve":       r["days_to_approve"],
    })

# ── Presidential era aggregates ───────────────────────────────────────────
ERA_MAP = {
    2001:"Bush T1",2002:"Bush T1",2003:"Bush T1",2004:"Bush T1",
    2005:"Bush T2",2006:"Bush T2",2007:"Bush T2",2008:"Bush T2",
    2009:"Obama T1",2010:"Obama T1",2011:"Obama T1",2012:"Obama T1",
    2013:"Obama T2",2014:"Obama T2",2015:"Obama T2",2016:"Obama T2",
    2017:"Trump T1",2018:"Trump T1",2019:"Trump T1",2020:"Trump T1",
    2021:"Biden",2022:"Biden",2023:"Biden",2024:"Biden",
    2025:"Trump T2",
}

era_dec_map = defaultdict(lambda: {"declarations": 0, "days": [], "incidents": defaultdict(int)})
for r in dec_valid:
    era = ERA_MAP.get(r["fyDeclared"])
    if not era:
        continue
    era_dec_map[era]["declarations"] += 1
    era_dec_map[era]["days"].append(r["days_to_approve"])
    era_dec_map[era]["incidents"][r["incidentType"] or "Unknown"] += 1

era_den_map = defaultdict(lambda: {"denials": 0, "days": []})
for r in den_valid:
    yr = int(r["declarationRequestDate"][:4]) if r["declarationRequestDate"] else 0
    era = ERA_MAP.get(yr)
    if not era:
        continue
    era_den_map[era]["denials"] += 1
    era_den_map[era]["days"].append(r["days_to_deny"])

def build_era_row(key, dec_d, den_d):
    decl    = dec_d.get("declarations", 0)
    denials = den_d.get("denials", 0)
    total_r = decl + denials
    return {
        "era":           key,
        "declarations":  decl,
        "denials":       denials,
        "total_requests": total_r,
        "denial_rate":   round(denials / total_r * 100, 2) if total_r else 0,
        "avg_days":      avg(dec_d.get("days", [])),
        "avg_deny_days": avg(den_d.get("days", [])),
    }

TERM_KEYS = ["Bush T1","Bush T2","Obama T1","Obama T2","Trump T1","Biden","Trump T2"]
era_rows  = {k: build_era_row(k, era_dec_map.get(k, {}), era_den_map.get(k, {})) for k in TERM_KEYS}

def combined_era(label, keys):
    all_dec  = sum(era_rows[k]["declarations"]  for k in keys if k in era_rows)
    all_den  = sum(era_rows[k]["denials"]        for k in keys if k in era_rows)
    all_tr   = all_dec + all_den
    all_d_days = [d for k in keys for d in era_dec_map.get(k, {}).get("days", [])]
    all_n_days = [d for k in keys for d in era_den_map.get(k, {}).get("days", [])]
    return {
        "era": label, "declarations": all_dec, "denials": all_den,
        "total_requests": all_tr,
        "denial_rate":    round(all_den / all_tr * 100, 2) if all_tr else 0,
        "avg_days":       avg(all_d_days),
        "avg_deny_days":  avg(all_n_days),
    }

era_ordered = [
    era_rows["Bush T1"], era_rows["Bush T2"], combined_era("Bush Total", ["Bush T1","Bush T2"]),
    era_rows["Obama T1"], era_rows["Obama T2"], combined_era("Obama Total", ["Obama T1","Obama T2"]),
    era_rows["Trump T1"], era_rows["Biden"],
    era_rows["Trump T2"], combined_era("Trump Total", ["Trump T1","Trump T2"]),
]

# Era incident breakdown
era_inc = {}
for key in list(TERM_KEYS) + ["Bush Total","Obama Total","Trump Total"]:
    src_keys = (["Bush T1","Bush T2"] if "Bush Total" in key else
                ["Obama T1","Obama T2"] if "Obama Total" in key else
                ["Trump T1","Trump T2"] if "Trump Total" in key else [key])
    combined_inc = defaultdict(int)
    for k in src_keys:
        for inc, cnt in era_dec_map.get(k, {}).get("incidents", {}).items():
            combined_inc[inc] += cnt
    era_inc[key] = sorted([{"type": inc, "count": cnt} for inc, cnt in combined_inc.items()],
                           key=lambda x: -x["count"])[:6]

# Era YoY
yoy_era = []
for r in dec_valid:
    era = ERA_MAP.get(r["fyDeclared"])
    if era:
        yoy_era.append({"fyDeclared": r["fyDeclared"], "era": era})

from collections import Counter
yoy_era_counts = Counter((r["fyDeclared"], r["era"]) for r in yoy_era)
yoy_era_list = [{"fyDeclared": fy, "era": era, "count": cnt}
                for (fy, era), cnt in sorted(yoy_era_counts.items())]

# Era disaster lists
era_disasters = {}
for key in list(TERM_KEYS) + ["Bush Total","Obama Total","Trump Total"]:
    src_keys = (["Bush T1","Bush T2"] if "Bush Total" in key else
                ["Obama T1","Obama T2"] if "Obama Total" in key else
                ["Trump T1","Trump T2"] if "Trump Total" in key else [key])
    recs = []
    seen3 = set()
    for r in sorted(dec_valid, key=lambda x: x["declarationDate"], reverse=True):
        era = ERA_MAP.get(r["fyDeclared"])
        if era not in src_keys:
            continue
        fid = r["femaDeclarationString"]
        if fid in seen3:
            continue
        seen3.add(fid)
        recs.append({"id":r["femaDeclarationString"],"state":r["state"],"dt":r["declarationType"],
                     "date":r["declarationDate"],"fy":r["fyDeclared"],"inc":r["incidentType"],
                     "title":r["declarationTitle"],"days":r["days_to_approve"],"reg":r["region"]})
    era_disasters[key] = recs

era_data = {
    "eraOrdered":   era_ordered,
    "eraInc":       era_inc,
    "yoyEra":       yoy_era_list,
    "eraDisasters": era_disasters,
    "eraDenials":   {},   # kept for schema compatibility
}

print("Aggregation complete.\n")


# ═════════════════════════════════════════════════════════════════════════
# 4. BUILD HTML
# ═════════════════════════════════════════════════════════════════════════

print("Building HTML...")

# Read the template and inject data
# All data is serialised to JSON and embedded directly in the HTML.
summary_json          = json.dumps(summary,          separators=(",",":"))
state_summary_json    = json.dumps(state_summary,    separators=(",",":"))
state_yoy_json        = json.dumps(state_yoy,        separators=(",",":"))
state_inc_json        = json.dumps(state_inc,        separators=(",",":"))
state_disasters_json  = json.dumps(dict(state_disasters), separators=(",",":"))
denials_json          = json.dumps(den_processed,    separators=(",",":"))
browse_json           = json.dumps(browse,           separators=(",",":"))
era_json              = json.dumps(era_data,         separators=(",",":"))

STATE_NAMES = {
    "AK":"Alaska","AL":"Alabama","AR":"Arkansas","AS":"American Samoa","AZ":"Arizona",
    "CA":"California","CO":"Colorado","CT":"Connecticut","DC":"Washington D.C.","DE":"Delaware",
    "FL":"Florida","FM":"Fed. States of Micronesia","GA":"Georgia","GU":"Guam","HI":"Hawaii",
    "IA":"Iowa","ID":"Idaho","IL":"Illinois","IN":"Indiana","KS":"Kansas","KY":"Kentucky",
    "LA":"Louisiana","MA":"Massachusetts","MD":"Maryland","ME":"Maine","MI":"Michigan",
    "MN":"Minnesota","MO":"Missouri","MP":"N. Mariana Islands","MS":"Mississippi","MT":"Montana",
    "NC":"North Carolina","ND":"North Dakota","NE":"Nebraska","NH":"New Hampshire","NJ":"New Jersey",
    "NM":"New Mexico","NV":"Nevada","NY":"New York","OH":"Ohio","OK":"Oklahoma","OR":"Oregon",
    "PA":"Pennsylvania","PR":"Puerto Rico","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota",
    "TN":"Tennessee","TX":"Texas","UT":"Utah","VA":"Virginia","VI":"U.S. Virgin Islands",
    "VT":"Vermont","WA":"Washington","WI":"Wisconsin","WV":"West Virginia","WY":"Wyoming",
}
state_names_json = json.dumps(STATE_NAMES, separators=(",",":"))

# Read the current index.html template (if it exists) or use a placeholder
# In the GitHub Actions workflow the previous index.html is already checked out
import os
template_path = "index.html"

if os.path.exists(template_path):
    with open(template_path, encoding="utf-8") as f:
        html = f.read()

    # Replace the embedded data blobs — they are assigned as JS constants
    import re

    def replace_const(html, const_name, new_json):
        pattern = rf'(const {re.escape(const_name)}\s*=\s*).*?;'
        replacement = rf'\g<1>{new_json};'
        return re.sub(pattern, replacement, html, count=1, flags=re.DOTALL)

    for name, blob in [
        ("SUMMARY",         summary_json),
        ("STATE_SUMMARY",   state_summary_json),
        ("STATE_YOY",       state_yoy_json),
        ("STATE_INC",       state_inc_json),
        ("STATE_DISASTERS", state_disasters_json),
        ("DENIALS",         denials_json),
        ("BROWSE",          browse_json),
        ("ERA",             era_json),
        ("STATE_NAMES",     state_names_json),
    ]:
        html = replace_const(html, name, blob)

    # Update the last-refreshed note in the About tab
    html = re.sub(r'Last updated:.*?(?=<)', f'Last updated: {TODAY}', html)

else:
    print("  WARNING: No index.html found — writing data only to data_snapshot.json")
    with open("data_snapshot.json", "w") as f:
        json.dump({
            "summary": summary, "stateSummary": state_summary,
            "stateYoy": state_yoy, "stateInc": state_inc,
            "stateDisasters": dict(state_disasters),
            "denials": den_processed, "browse": browse,
            "era": era_data,
        }, f, separators=(",",":"))
    print("  Wrote data_snapshot.json. Re-run after placing index.html in this directory.")
    exit(0)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

size_kb = len(html) // 1024
print(f"  index.html written ({size_kb} KB)")
print(f"\nDone. Data as of {TODAY}.")
print(f"  Declarations: {len(dec_processed):,}")
print(f"  Denials:      {len(den_processed):,}")
print(f"  Browse items: {len(browse):,}")
