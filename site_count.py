"""Daily site-count: log the totals the website reports for its own listings.

Writes (via rclone, gdrive:taladnudbaan/):
  site_count_log.csv        date_ict, timestamp_utc, site_total
  site_count_breakdown.csv  date_ict, level, province_id, province_name, type_id, type_slug, sale_method, total

level       overall | type | province | province_type
sale_method all | sale (sale_method=1) | auction (sale_method=2); province_type 'all' rows = sale + auction

Env: SITE_COUNT_DRY_RUN=1 skips rclone (writes the CSVs only in the working directory).
"""
import collections
import datetime
import os
import re
import subprocess
import sys
import time
import urllib.request
import zoneinfo
from concurrent.futures import ThreadPoolExecutor

BASE = "https://www.taladnudbaan.com/properties?sellers_only_out=on&view=list&page_length=20&page=1"
UA = {"User-Agent": "Mozilla/5.0 (research data collection; contact: limhengue@gmail.com)"}
TYPES = list(range(1, 10))
PROVINCE_IDS = list(range(1, 151))   # ids with no listings (total=0) are skipped
SM = {"all": "", "sale": "&sale_method=1", "auction": "&sale_method=2"}
DRY = os.environ.get("SITE_COUNT_DRY_RUN") == "1"
MAX_FAILED = 30


def info(extra):
    """Return (total, most common type slug, most common province name) or None if the request keeps failing."""
    for attempt in range(3):
        try:
            req = urllib.request.Request(BASE + extra, headers=UA)
            html = urllib.request.urlopen(req, timeout=60).read().decode("utf-8", "ignore")
            text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))
            m = re.search(r"ผลการค้นหา\s*:\s*([\d,]+)", text)
            total = int(m.group(1).replace(",", "")) if m else 0
            slugs = collections.Counter(re.findall(r'href="[^"]*?/property/([^/"]+)/[^/"]+/[^/"]+"', html))
            provs = collections.Counter(re.findall(r"\s,\s*([^\s,][^,]*?)\s+[\d,]{4,}\s*บาท", text))
            time.sleep(0.2)
            return (total,
                    slugs.most_common(1)[0][0] if slugs else "",
                    provs.most_common(1)[0][0] if provs else "")
        except Exception:
            time.sleep(5 * (attempt + 1))
    return None


def rclone(*args, check=False):
    if DRY:
        return
    subprocess.run(["rclone", *args], check=check)


def append(remote, local, header, new_lines):
    rclone("copyto", remote, local)   # no remote file yet = start fresh
    try:
        txt = open(local, encoding="utf-8").read()
    except FileNotFoundError:
        txt = ""
    if not txt.strip():
        txt = header + "\n"
    if not txt.endswith("\n"):
        txt += "\n"
    open(local, "w", encoding="utf-8").write(txt + "\n".join(new_lines) + "\n")
    rclone("copyto", local, remote, check=True)


def quote(v):
    v = str(v)
    return '"' + v.replace('"', '""') + '"' if "," in v else v


def main():
    now = datetime.datetime.now(datetime.timezone.utc)
    ict = now.astimezone(zoneinfo.ZoneInfo("Asia/Bangkok")).date().isoformat()

    overall = info("")
    if overall is None or overall[0] == 0:
        sys.exit("cannot read the overall site total (layout changed or blocked?)")

    rows = []     # (date, level, province_id, province_name, type_id, type_slug, sale_method, total)
    failed = 0

    def add(level, pid, pname, tid, slug, sm, r):
        nonlocal failed
        if r is None:
            failed += 1
            rows.append((ict, level, pid, pname, tid, slug, sm, ""))
        elif r[0] > 0:
            rows.append((ict, level, pid, pname, tid, slug, sm, r[0]))

    with ThreadPoolExecutor(4) as ex:
        # overall, then by type, each for all / sale / auction
        for sm, extra in SM.items():
            add("overall", "", "", "", "", sm, overall if sm == "all" else info(extra))
        type_jobs = [(t, sm) for t in TYPES for sm in SM]
        for (t, sm), r in zip(type_jobs, ex.map(lambda j: info(f"&type_id={j[0]}{SM[j[1]]}"), type_jobs)):
            add("type", "", "", t, (r[1] if r else ""), sm, r)

        # provinces: probe every id with sm=all, then sale / auction only for provinces that have listings
        pall = list(ex.map(lambda p: info(f"&province_id={p}"), PROVINCE_IDS))
        live = []
        for p, r in zip(PROVINCE_IDS, pall):
            if r is None or r[0] > 0:
                add("province", p, (r[2] if r else ""), "", "", "all", r)
                if r is not None:
                    live.append((p, r[2]))
        pjobs = [(p, pn, sm) for p, pn in live for sm in ("sale", "auction")]
        for (p, pn, sm), r in zip(pjobs, ex.map(lambda j: info(f"&province_id={j[0]}{SM[j[2]]}"), pjobs)):
            add("province", p, pn, "", "", sm, r)

        # province x type for sale / auction; the 'all' row is their sum
        combos = [(p, pn, t, sm) for p, pn in live for t in TYPES for sm in ("sale", "auction")]
        cres = list(ex.map(lambda c: info(f"&province_id={c[0]}&type_id={c[2]}{SM[c[3]]}"), combos))
        acc = collections.defaultdict(int)
        for (p, pn, t, sm), r in zip(combos, cres):
            add("province_type", p, pn, t, (r[1] if r else ""), sm, r)
            if r is not None:
                acc[(p, pn, t)] += r[0]
        for (p, pn, t), tot in acc.items():
            if tot > 0:
                rows.append((ict, "province_type", p, pn, t, "", "all", tot))

    print(f"overall={overall[0]} rows={len(rows)} failed_requests={failed}")
    if failed > MAX_FAILED:
        sys.exit(f"too many failed requests ({failed}) - not writing logs")

    append("gdrive:taladnudbaan/site_count_log.csv", "site_count_log.csv",
           "date_ict,timestamp_utc,site_total",
           [f"{ict},{now.strftime('%Y-%m-%dT%H:%M:%SZ')},{overall[0]}"])
    append("gdrive:taladnudbaan/site_count_breakdown.csv", "site_count_breakdown.csv",
           "date_ict,level,province_id,province_name,type_id,type_slug,sale_method,total",
           [",".join(quote(c) for c in r) for r in rows])
    print("done")


if __name__ == "__main__":
    main()
