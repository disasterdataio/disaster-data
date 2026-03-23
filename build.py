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
    base_filter = f"fyDeclared ge {START_YEAR}" if endpoint == "DisasterDeclarationsSummaries" else f"declarationRequestDate ge '{START_YEAR}-01-01T00:00:00.000Z'"

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
            f"&$orderby=id%20asc"
            + select_param
        )
        url = f"{BASE_URL}/{endpoint}{params}"

        for attempt in range(5):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "UWSWVA-FEMA-Explorer/1.0"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read())
                break
            except Exception as e:
                if attempt == 4:
                    raise
                wait = 10 * (attempt + 1)
                print(f"    Retry {attempt+1}/4 after error: {e} (waiting {wait}s)")
                time.sleep(wait)

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
try:
    raw_dec = fetch_all("DisasterDeclarationsSummaries", fields=DEC_FIELDS)
    print(f"  → {len(raw_dec)} declaration records\n")
except Exception as e:
    print(f"  ERROR: Declarations fetch failed after retries: {e}")
    print("  Cannot continue without declarations data — exiting.")
    raise SystemExit(1)

print("Fetching denials...")
try:
    raw_den = fetch_all("DeclarationDenials", extra_filter="currentRequestStatus eq 'Turndown'", fields=DEN_FIELDS)
    print(f"  → {len(raw_den)} denial records\n")
except Exception as e:
    print(f"  WARNING: Denials fetch failed: {e}")
    print("  Trying without status filter...")
    try:
        raw_den = fetch_all("DeclarationDenials", fields=DEN_FIELDS)
        print(f"  → {len(raw_den)} denial records\n")
    except Exception as e2:
        print(f"  WARNING: Denials unavailable: {e2}\n")
        raw_den = []


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
# 2b. FETCH PUBLIC ASSISTANCE NATIONAL SUMMARY
# ═════════════════════════════════════════════════════════════════════════

PA_BASE    = "https://www.fema.gov/api/open/v2"
PA_FIELDS  = [
    "disasterNumber", "stateAbbreviation", "federalShareObligated",
    "totalObligated", "damageCategoryCode", "damageCategoryDescrip",
    "declarationDate", "incidentType"
]

def fetch_pa_all():
    """Fetch all PA funded projects details (2000+) for national summary."""
    records = []
    skip    = 0
    total   = None
    filt    = urllib.parse.quote("declarationDate ge '2000-01-01T00:00:00.000Z'")
    select  = ",".join(PA_FIELDS)
    print("  Fetching PublicAssistanceFundedProjectsDetails (national)...")

    while True:
        url = (f"{PA_BASE}/PublicAssistanceFundedProjectsDetails"
               f"?$top={PAGE_SIZE}&$skip={skip}"
               f"&$filter={filt}"
               f"&$select={select}"
               f"&$inlinecount=allpages")

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "DisasterData-Explorer/1.0"})
                with urllib.request.urlopen(req, timeout=90) as resp:
                    data = json.loads(resp.read())
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"    Retry {attempt+1}: {e}")
                time.sleep(5)

        batch = data.get("PublicAssistanceFundedProjectsDetails", [])
        records.extend(batch)

        if total is None:
            total = int(data.get("metadata", {}).get("count", 0))
            print(f"    Total PA records: {total:,}")

        skip += len(batch)
        if skip % 50000 == 0 or skip >= total:
            print(f"    Fetched {skip:,}/{total:,}")

        if not batch or (total and skip >= total):
            break
        time.sleep(SLEEP_SEC)

    return records

print("Fetching PA data (this may take several minutes)...")
try:
    raw_pa = fetch_pa_all()
    print(f"  → {len(raw_pa):,} PA project records\n")
    PA_AVAILABLE = True
except Exception as e:
    print(f"  WARNING: PA fetch failed: {e}")
    print("  PA national summary will be skipped.\n")
    raw_pa = []
    PA_AVAILABLE = False

# Build PA national summary aggregates
pa_national = {}
if raw_pa:
    pa_by_state   = {}
    pa_by_cat     = {}
    pa_by_disaster = {}
    pa_total_obl  = 0
    pa_total_proj = 0
    pa_disasters  = set()

    DCC_LABELS = {
        "A":"Debris Removal","B":"Emergency Protective Measures",
        "C":"Roads & Bridges","D":"Water Control Facilities",
        "E":"Buildings & Equipment","F":"Utilities",
        "G":"Parks, Recreational, and Other Items","Z":"Management Costs"
    }

    for r in raw_pa:
        st   = r.get("stateAbbreviation") or "Unknown"
        obl  = float(r.get("federalShareObligated") or 0)
        tot  = float(r.get("totalObligated") or 0)
        cat  = r.get("damageCategoryDescrip") or DCC_LABELS.get(r.get("damageCategoryCode",""), "Other")
        dn   = r.get("disasterNumber")

        pa_total_obl  += obl
        pa_total_proj += 1
        if dn: pa_disasters.add(dn)

        if st not in pa_by_state:
            pa_by_state[st] = {"obl": 0, "tot": 0, "proj": 0}
        pa_by_state[st]["obl"]  += obl
        pa_by_state[st]["tot"]  += tot
        pa_by_state[st]["proj"] += 1

        if cat not in pa_by_cat:
            pa_by_cat[cat] = {"obl": 0, "proj": 0}
        pa_by_cat[cat]["obl"]  += obl
        pa_by_cat[cat]["proj"] += 1

    # Top 15 states by federal share
    top_states = sorted(
        [{"state": k, "obl": round(v["obl"],2), "proj": v["proj"]} for k,v in pa_by_state.items()],
        key=lambda x: -x["obl"]
    )[:15]

    # All categories sorted by federal share
    top_cats = sorted(
        [{"cat": k, "obl": round(v["obl"],2), "proj": v["proj"]} for k,v in pa_by_cat.items()],
        key=lambda x: -x["obl"]
    )

    # Largest single project
    largest = max((float(r.get("federalShareObligated") or 0) for r in raw_pa), default=0)

    pa_national = {
        "totalObligated":  round(pa_total_obl, 2),
        "totalProjects":   pa_total_proj,
        "totalDisasters":  len(pa_disasters),
        "largestProject":  round(largest, 2),
        "topState":        top_states[0]["state"] if top_states else "—",
        "topCategory":     top_cats[0]["cat"] if top_cats else "—",
        "topStates":       top_states,
        "topCategories":   top_cats,
    }
    print(f"  PA summary: ${pa_total_obl/1e9:.1f}B total, {len(pa_disasters):,} disasters, {pa_total_proj:,} projects")



# ═════════════════════════════════════════════════════════════════════════
# 2c. FETCH HAZARD MITIGATION GRANTS NATIONAL SUMMARY
# ═════════════════════════════════════════════════════════════════════════

HM_FIELDS = [
    # Correct field names from OpenFEMA HazardMitigationAssistanceProjects v4 data dictionary
    "programArea", "state", "federalShareObligated",
    "typeOfProject", "subrecipient", "disasterNumber", "programFy", "region"
]

def classify_hm(r):
    # programArea values are exact short codes: HMGP, FMA, BRIC, PDM, RFC, SRL
    prog = (r.get("programArea") or "").strip().upper()
    if prog in ("HMGP",): return "HMGP"
    if prog in ("BRIC",): return "BRIC"
    if prog in ("FMA",):  return "FMA"
    # PDM is legacy (replaced by BRIC 2020), RFC/SRL folded into FMA — exclude
    return None

def fetch_hm_all():
    """Fetch all HazardMitigationAssistanceProjects records for national summary."""
    records = []
    skip    = 0
    total   = None
    select  = ",".join(HM_FIELDS)
    print("  Fetching HazardMitigationAssistanceProjects (national)...")

    while True:
        url = (f"{BASE_URL}/HazardMitigationAssistanceProjects"
               f"?$top={PAGE_SIZE}&$skip={skip}"
               f"&$select={select}"
               f"&$inlinecount=allpages")

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "DisasterData-Explorer/1.0"})
                with urllib.request.urlopen(req, timeout=90) as resp:
                    data = json.loads(resp.read())
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"    Retry {attempt+1}: {e}")
                time.sleep(5)

        # Correct response key matches dataset name
        batch = data.get("HazardMitigationAssistanceProjects", [])
        records.extend(batch)

        if total is None:
            total = int(data.get("metadata", {}).get("count", 0))
            print(f"    Total HM records: {total:,}")

        skip += len(batch)
        if skip % 50000 == 0 or (total and skip >= total):
            print(f"    Fetched {skip:,}/{total:,}")

        if not batch or (total and skip >= total):
            break
        time.sleep(SLEEP_SEC)

    return records

print("Fetching Hazard Mitigation grant data...")
try:
    raw_hm = fetch_hm_all()
    print(f"  → {len(raw_hm):,} HM grant records\n")
    HM_AVAILABLE = True
except Exception as e:
    print(f"  WARNING: HM fetch failed: {e}\n")
    raw_hm = []
    HM_AVAILABLE = False

# Build HM national summary aggregates per program
def agg_hm_program(records):
    by_state = defaultdict(float)
    by_type  = defaultdict(float)
    total_obl = 0.0
    subgrantees = set()
    projects = 0
    for r in records:
        st  = r.get("state") or "Unknown"
        obl = float(r.get("federalShareObligated") or 0)
        typ = (r.get("typeOfProject") or "Other").strip()
        by_state[st] += obl
        by_type[typ]  += obl
        total_obl    += obl
        projects     += 1
        sg = r.get("subrecipient")
        if sg: subgrantees.add(sg)
    top_states = sorted([{"state": k, "obl": round(v, 2)} for k,v in by_state.items()],
                        key=lambda x: -x["obl"])[:15]
    top_types  = sorted([{"type": k, "obl": round(v, 2)} for k,v in by_type.items()],
                        key=lambda x: -x["obl"])[:10]
    return {
        "totalObligated":  round(total_obl, 2),
        "totalProjects":   projects,
        "subgrantees":     len(subgrantees),
        "topState":        top_states[0]["state"] if top_states else "—",
        "topStates":       top_states,
        "topTypes":        top_types,
    }

hm_national = {}
if raw_hm:
    buckets = {"HMGP": [], "BRIC": [], "FMA": []}
    for r in raw_hm:
        bucket = classify_hm(r)
        if bucket:
            buckets[bucket].append(r)

    hm_national = {
        "HMGP": agg_hm_program(buckets["HMGP"]),
        "BRIC": agg_hm_program(buckets["BRIC"]),
        "FMA":  agg_hm_program(buckets["FMA"]),
    }
    total_all = sum(hm_national[p]["totalObligated"] for p in hm_national)
    print(f"  HM summary: ${total_all/1e9:.1f}B total | "
          f"HMGP {len(buckets['HMGP']):,} | BRIC {len(buckets['BRIC']):,} | FMA {len(buckets['FMA']):,}")



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

# Helper for grouping by state
def groupby_state(records):
    from collections import defaultdict
    state_map = defaultdict(list)
    for r in records:
        state_map[r.get("state","")].append(r)
    return state_map.items()

# 4. BUILD data.js
# ═════════════════════════════════════════════════════════════════════════

print("Building data.js...")

import re, os

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

# Presidential term FY mapping (for client-side era filtering)
ERA_FY_MAP = {
    2001:"bush_t1",2002:"bush_t1",2003:"bush_t1",2004:"bush_t1",
    2005:"bush_t2",2006:"bush_t2",2007:"bush_t2",2008:"bush_t2",
    2009:"obama_t1",2010:"obama_t1",2011:"obama_t1",2012:"obama_t1",
    2013:"obama_t2",2014:"obama_t2",2015:"obama_t2",2016:"obama_t2",
    2017:"trump_t1",2018:"trump_t1",2019:"trump_t1",2020:"trump_t1",
    2021:"biden",2022:"biden",2023:"biden",2024:"biden",
    2025:"trump_t2",
}
ERA_TOTAL_KEYS = {
    "bush_total":  ["bush_t1","bush_t2"],
    "obama_total": ["obama_t1","obama_t2"],
    "trump_total": ["trump_t1","trump_t2"],
}

# Build era_data for PRES_DATA (without disaster lists — those come from BROWSE)
era_dec_map  = defaultdict(lambda: {"declarations": 0, "days": []})
era_den_map  = defaultdict(lambda: {"denials": 0, "days": []})

for r in dec_valid:
    era = ERA_FY_MAP.get(r["fyDeclared"])
    if era:
        era_dec_map[era]["declarations"] += 1
        era_dec_map[era]["days"].append(r["days_to_approve"])

for r in den_valid:
    yr  = int(r["declarationRequestDate"][:4]) if r["declarationRequestDate"] else 0
    era = ERA_FY_MAP.get(yr)
    if era:
        era_den_map[era]["denials"] += 1
        era_den_map[era]["days"].append(r["days_to_deny"])

def era_stats_dict(keys):
    d  = sum(era_dec_map[k]["declarations"] for k in keys)
    dn = sum(era_den_map[k]["denials"]      for k in keys)
    tr = d + dn
    dd = [x for k in keys for x in era_dec_map[k]["days"]]
    nd = [x for k in keys for x in era_den_map[k]["days"]]
    return {
        "declarations": d, "denials": dn, "total_requests": tr,
        "denial_rate":  round(dn/tr*100, 2) if tr else 0,
        "avg_days":     round(sum(dd)/len(dd), 1) if dd else 0,
        "avg_deny_days":round(sum(nd)/len(nd), 1) if nd else 0,
    }

YEARS_MAP = {
    "bush_t1":"2001-2004","bush_t2":"2005-2008","bush_total":"2001-2008",
    "obama_t1":"2009-2012","obama_t2":"2013-2016","obama_total":"2009-2016",
    "trump_t1":"2017-2020","biden":"2021-2024",
    "trump_t2":"2025 (partial)","trump_total":"2017-2020 + 2025",
}
LABEL_MAP = {
    "bush_t1":"Bush T1","bush_t2":"Bush T2","bush_total":"Bush Total",
    "obama_t1":"Obama T1","obama_t2":"Obama T2","obama_total":"Obama Total",
    "trump_t1":"Trump T1","biden":"Biden",
    "trump_t2":"Trump T2","trump_total":"Trump Total",
}
TERM_KEYS = ["bush_t1","bush_t2","obama_t1","obama_t2","trump_t1","biden","trump_t2"]

pres_data = {}
for group_keys, key in [
    (["bush_t1"],              "bush_t1"),
    (["bush_t2"],              "bush_t2"),
    (["bush_t1","bush_t2"],    "bush_total"),
    (["obama_t1"],             "obama_t1"),
    (["obama_t2"],             "obama_t2"),
    (["obama_t1","obama_t2"],  "obama_total"),
    (["trump_t1"],             "trump_t1"),
    (["biden"],                "biden"),
    (["trump_t2"],             "trump_t2"),
    (["trump_t1","trump_t2"],  "trump_total"),
]:
    stats = era_stats_dict(group_keys)
    # Top incident types for this era
    inc_counter = defaultdict(int)
    for r in dec_valid:
        if ERA_FY_MAP.get(r["fyDeclared"]) in group_keys:
            inc_counter[r["incidentType"] or "Unknown"] += 1
    top_inc = sorted([{"type":k,"count":v} for k,v in inc_counter.items()],
                     key=lambda x: -x["count"])[:6]
    pres_data[key] = {
        "label":         LABEL_MAP[key],
        "years":         YEARS_MAP[key],
        "declarations":  stats["declarations"],
        "denials":       stats["denials"],
        "total":         stats["total_requests"],
        "denial_rate":   stats["denial_rate"],
        "avg_days":      stats["avg_days"],
        "avg_deny_days": stats["avg_deny_days"],
        "top_incidents": top_inc,
        # disasters intentionally omitted — filtered from BROWSE client-side
    }

PRES_ORDER = [
    ["bush_t1","Bush — Term 1","2001-2004"],
    ["bush_t2","Bush — Term 2","2005-2008"],
    ["bush_total","Bush — Total","2001-2008"],
    ["obama_t1","Obama — Term 1","2009-2012"],
    ["obama_t2","Obama — Term 2","2013-2016"],
    ["obama_total","Obama — Total","2009-2016"],
    ["trump_t1","Trump — Term 1","2017-2020"],
    ["biden","Biden","2021-2024"],
    ["trump_t2","Trump — Term 2","2025 (partial)"],
    ["trump_total","Trump — Total","2017-2020 + 2025"],
]

# Build locality data (compact: IDs only, client looks up in BROWSE)
locality_data = defaultdict(list)
for state, grp in groupby_state(dec_valid):
    loc_map = defaultdict(lambda: {"rows": [], "ids": set()})
    for r in grp:
        area = r.get("designatedArea", "") or ""
        loc_map[area]["rows"].append(r)
        loc_map[area]["ids"].add(r["femaDeclarationString"])
    locs = []
    for area, v in loc_map.items():
        rows = v["rows"]
        top_inc = max(set(r["incidentType"] for r in rows if r["incidentType"]),
                      key=lambda x: sum(1 for r in rows if r["incidentType"]==x),
                      default="")
        locs.append({
            "n":   area,
            "c":   len(rows),
            "d":   len(v["ids"]),
            "a":   round(sum(r["days_to_approve"] for r in rows)/len(rows), 1),
            "l":   max(r["declarationDate"] for r in rows),
            "t":   top_inc,
            "ids": sorted(v["ids"]),
        })
    locality_data[state] = sorted(locs, key=lambda x: -x["c"])

# Write data.js — all window.VAR = ... assignments
lines = [
    f'window.SUMMARY          ={json.dumps(summary,         separators=(",",":"))}',
    f'window.STATE_SUMMARY    ={json.dumps(state_summary,   separators=(",",":"))}',
    f'window.STATE_YOY        ={json.dumps(state_yoy,       separators=(",",":"))}',
    f'window.STATE_INC        ={json.dumps(state_inc,       separators=(",",":"))}',
    f'window.STATE_DISASTERS  ={{}}',
    f'window.DENIALS          ={json.dumps(den_processed,   separators=(",",":"))}',
    f'window.BROWSE           ={json.dumps(browse,          separators=(",",":"))}',
    f'window.STATE_NAMES      ={json.dumps(STATE_NAMES,     separators=(",",":"))}',
    f'window.LOCALITY_DATA    ={json.dumps(dict(locality_data), separators=(",",":"))}',
    f'window.PRES_DATA        ={json.dumps(pres_data,       separators=(",",":"))}',
    f'window.PRES_ORDER       ={json.dumps(PRES_ORDER,      separators=(",",":"))}',
    f'window.ERA_FY_MAP       ={json.dumps(ERA_FY_MAP,      separators=(",",":"))}',
    f'window.ERA_TOTAL_KEYS   ={json.dumps(ERA_TOTAL_KEYS,  separators=(",",":"))}',
    f'window.DATA_DATE        ="{TODAY}"',
    f'window.PA_NATIONAL      ={json.dumps(pa_national,  separators=(",",":"))}',
    f'window.HM_NATIONAL      ={json.dumps(hm_national,  separators=(",",":"))}',
    "document.dispatchEvent(new Event('dataReady'));",
]

data_js_content = "\n".join(lines)

with open("data.js", "w", encoding="utf-8") as f:
    f.write(data_js_content)

data_kb = len(data_js_content) // 1024
print(f"  data.js written ({data_kb} KB)")

# Update index.html: inject PA_NATIONAL and refresh last-updated stamp
if os.path.exists("index.html"):
    with open("index.html", encoding="utf-8") as f:
        html = f.read()
    # Update last-updated stamp
    html = re.sub(r'Last updated:.*?(?=<)', f'Last updated: {TODAY}', html)
    # Inject PA_NATIONAL and HM_NATIONAL using line-based replacement
    # (regex approach breaks when JSON contains semicolons in string values)
    pa_json = json.dumps(pa_national, separators=(",",":"))
    # Only inject HM_NATIONAL if we have real data — never overwrite baked-in data with {}
    hm_json = json.dumps(hm_national, separators=(",",":")) if hm_national else None
    lines_out = []
    for line in html.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith('let PA_NATIONAL') and stripped.endswith(';'):
            line = f'let PA_NATIONAL = {pa_json};\n'
        elif stripped.startswith('let HM_NATIONAL') and stripped.endswith(';') and hm_json:
            line = f'let HM_NATIONAL = {hm_json};\n'
        lines_out.append(line)
    html = ''.join(lines_out)
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("  index.html updated with PA_NATIONAL and last-updated stamp")

print(f"\nDone. Data as of {TODAY}.")
print(f"  Declarations: {len(dec_processed):,}")
print(f"  Denials:      {len(den_processed):,}")
print(f"  Browse items: {len(browse):,}")
print(f"  Localities:   {sum(len(v) for v in locality_data.values()):,}")
