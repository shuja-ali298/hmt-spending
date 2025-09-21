#!/usr/bin/env python3
import argparse, json, os, re
from datetime import date, datetime
from urllib.parse import urljoin
from dateutil.relativedelta import relativedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

COL_MAPS = {
    "department_family": ["department family","department","departmentfamily"],
    "entity": ["entity","body","entity name"],
    "date": ["payment date","date"],
    "expense_type": ["expense type","expenditure type","type"],
    "expense_area": ["expense area","cost centre","expensearea"],
    "supplier": ["supplier","vendor","supplier name"],
    "transaction_number": ["voucher number","transaction number","transaction no","transaction id"],
    "amount_gbp": [
        "amount", "amount gbp", "amount (gbp)", "amount £", "amount(£)",
        "net amount", "net amount gbp", "net amount (£)", "net value",
        "value", "line amount", "line value", "gross amount",
        "amount inc vat", "amount (inc vat)", "amt", "amount_gbp", "amountgbp"
    ],
    "description": ["publication description","description","item text","narrative"],
    "supplier_postcode": ["supplier postcode","postal code","post code","postcode"],
    "supplier_type": ["supplier type","supplier category"],
    "contract_number": ["contract number","contract no","po number","purchase order"],
    "project_code": ["project code","project","cost code"],
    "item_text": ["item text"],
}
PUB_URL_TMPL = "https://www.gov.uk/government/publications/hmt-spend-greater-than-25000-{month}-{year}"
MONTHS = ["january","february","march","april","may","june","july","august","september","october","november","december"]
HEADERS = {"User-Agent":"github-action-hmt-spend-json/1.0 (+https://github.com/)"}

def month_iter(start: date, end: date):
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        yield cur
        cur += relativedelta(months=1)

def find_asset_xlsx_or_csv(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    def normalize(href: str) -> str:
        return urljoin("https://www.gov.uk", href)

    for a in soup.select("a.gem-c-attachment__link, a.govuk-link.gem-c-attachment__link"):
        href = a.get("href", "")
        if re.search(r"\.(xlsx|csv)(?:\?.*)?$", href, flags=re.I):
            return normalize(href)
    return None

def _canon(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return re.sub(r"[^a-z0-9]", "", s)  # only a-z0-9

def smart_find(cols_raw, candidates):
    """
    Canonicalize headers and try exact/partial matches against candidate aliases.
    """
    cols_canon = [_canon(c) for c in cols_raw]
    cand_canon = [_canon(c) for c in candidates]

    # exact canonical match
    for cc in cand_canon:
        if cc in cols_canon:
            return cols_raw[cols_canon.index(cc)]

    # partial contains (e.g. 'amountgbp' inside 'amountgbpnet')
    for i, cc in enumerate(cand_canon):
        if not cc:
            continue
        for j, colc in enumerate(cols_canon):
            if cc in colc:
                return cols_raw[j]
    return None

def _parse_amount_series(s: pd.Series) -> pd.Series:
    """
    Convert diverse currency string formats to float.
    """
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce")

    t = s.astype(str)

    # normalize unicode dashes to ASCII minus
    t = (t.str.replace("\u2012", "-", regex=False)
           .str.replace("\u2013", "-", regex=False)
           .str.replace("\u2014", "-", regex=False))

    # strip currency symbols, commas, NBSPs, trim
    t = (t.str.replace("£", "", regex=False)
           .str.replace(",", "", regex=False)
           .str.replace("\u00A0", " ", regex=False)
           .str.strip())

    # accounting negatives: (1234.56) -> -1234.56
    t = t.str.replace(r"^\((.*)\)$", r"-\1", regex=True)

    # trailing minus: 1234- -> -1234
    t = t.str.replace(r"^(\d+(?:\.\d+)?)\-$", r"-\1", regex=True)

    # extract first numeric token
    num = t.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(num, errors="coerce")

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # widen aliases for 'amount' just before mapping
    COL_MAPS["amount_gbp"] = [
        "amount", "amount gbp", "amount (gbp)", "amount £", "£", "gbp",
        "net amount", "net amount gbp", "net value", "value",
        "gross amount", "line amount", "amount_gbp", "amt"
    ]

    cols_raw = [str(c) for c in df.columns]
    mapping = {}
    for k, aliases in COL_MAPS.items():
        found = smart_find(cols_raw, aliases)
        if found:
            mapping[k] = found

    out = pd.DataFrame(index=df.index.copy())
    for k in COL_MAPS.keys():
        out[k] = df[mapping[k]] if k in mapping else pd.NA

    # parse date if present
    if "date" in out and out["date"].notna().any():
        out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # parse amount robustly
    if "amount_gbp" in out:
        out["amount_gbp"] = _parse_amount_series(out["amount_gbp"])

    # drop rows utterly empty on key fields
    key_cols = [c for c in ["supplier", "amount_gbp", "date", "description"] if c in out.columns]
    if key_cols:
        out = out[~out[key_cols].isna().all(axis=1)]

    # optional: tiny debug to verify mapping (comment out once happy)
    # print("Header mapping:", mapping)

    return out

def save_month_json(dt: date, asset_url: str):
    os.makedirs(f"data/hmt/{dt.year}", exist_ok=True)
    out_path = f"data/hmt/{dt.year}/{dt.strftime('%Y-%m')}.json"
    r = requests.get(asset_url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    tmp = "/tmp/hmt_asset"
    open(tmp,"wb").write(r.content)

    if asset_url.lower().endswith(".csv"):
        df = pd.read_csv(tmp)
    else:
        xls = pd.ExcelFile(tmp)
        sheet = xls.sheet_names[0]
        df = pd.read_excel(xls, sheet_name=sheet)

    norm = normalize_dataframe(df)
    meta = {
        "source": asset_url,
        "publisher": "HM Treasury",
        "license": "Open Government Licence v3.0",
        "generated_at": datetime.utcnow().isoformat()+"Z",
        "rows": int(len(norm)),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(norm.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
    with open(out_path.replace(".json",".meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"Wrote {out_path} ({meta['rows']} rows)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", help="YYYY-MM (default: previous month)")
    ap.add_argument("--end", help="YYYY-MM (default: start)")
    a = ap.parse_args()

    today = date.today()
    default_start = (today.replace(day=1) - relativedelta(months=1))
    from datetime import datetime as dt
    start = dt.strptime(a.start, "%Y-%m").date().replace(day=1) if a.start else default_start
    end   = dt.strptime(a.end,   "%Y-%m").date().replace(day=1) if a.end   else start

    for dtm in month_iter(start, end):
        pub_url = PUB_URL_TMPL.format(month=MONTHS[dtm.month-1], year=dtm.year)
        try:
            pr = requests.get(pub_url, headers=HEADERS, timeout=30)
            if pr.status_code != 200:
                print(f"Skip {dtm:%Y-%m}: {pr.status_code} {pub_url}")
                continue
            asset = find_asset_xlsx_or_csv(pr.text)
            if not asset:
                print(f"No spreadsheet link found on {pub_url}")
                continue
            if asset.startswith("/"):
                asset = "https://www.gov.uk"+asset
            save_month_json(dtm, asset)
        except Exception as e:
            print(f"Error processing {dtm:%Y-%m}: {e}")

if __name__ == "__main__":
    main()
