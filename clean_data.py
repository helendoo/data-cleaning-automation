# clean_data.py
import pandas as pd
import re
import unicodedata
import difflib
from collections import Counter, defaultdict
from datetime import datetime
import csv

INPUT_FILE = "messy_customer_data.csv"
OUTPUT_FILE = "cleaned_customer_data.csv"

# ---------- helpers ----------
def maybe_demojibake(x: str) -> str:
    if not isinstance(x, str): return x
    if "Ã" in x or "Â" in x:
        try: return x.encode("latin-1").decode("utf-8")
        except Exception: return x
    return x

def strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in norm if not unicodedata.combining(ch))

def tidy_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).map(maybe_demojibake).str.strip()
        df[c] = df[c].str.replace(r"\s{2,}", " ", regex=True)
    return df

# ---------- name ----------
def clean_name(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().str.title()

# ---------- city ----------
def _basic_city_clean(x: str) -> str:
    x = strip_accents(str(x)).lower().strip()
    x = re.sub(r"[_\-\.,/]+", " ", x)
    x = re.sub(r"\s{2,}", " ", x)
    return x

def _letters_only_signature(x: str) -> str:
    return re.sub(r"[^a-z]", "", x)

def _choose_display_form(cands) -> str:
    top = Counter(cands).most_common(1)[0][0]
    return top if re.search(r"[A-Z]", top) else top.title()

def build_city_canon(series: pd.Series, fuzzy_cutoff: float = 0.92) -> dict:
    s_raw = series.fillna("").astype(str)
    s_trim = s_raw.map(maybe_demojibake).str.strip()
    s_basic = s_trim.map(_basic_city_clean)
    sigs = s_basic.map(_letters_only_signature)
    tmp = pd.DataFrame({"raw": s_trim, "basic": s_basic, "sig": sigs})
    groups = {}
    for _, r in tmp.iterrows():
        if r["basic"]:
            groups.setdefault(r["sig"], []).append(r["raw"])
    sig_to_disp = {sig: _choose_display_form(vals) for sig, vals in groups.items()}
    sizes = {sig: len(v) for sig, v in groups.items()}
    ordered = sorted(sizes, key=lambda k: sizes[k], reverse=True)
    merged = {}
    for small in ordered[::-1]:
        anchors = [s for s in ordered if sizes[s] >= sizes[small] and s != small]
        m = difflib.get_close_matches(small, anchors, n=1, cutoff=fuzzy_cutoff)
        if m: merged[small] = m[0]
    def root(sig):
        while sig in merged: sig = merged[sig]
        return sig
    final_disp = {sig: sig_to_disp.get(root(sig), sig_to_disp[sig]) for sig in sig_to_disp}
    mapping = {}
    for _, r in tmp.iterrows():
        if r["basic"]:
            mapping[r["raw"]] = final_disp[r["sig"]]
    return mapping

def clean_city(s: pd.Series) -> pd.Series:
    m = build_city_canon(s)
    out = s.astype(str).map(maybe_demojibake).str.strip().replace("", pd.NA)
    out = out.map(lambda v: m.get(v, v) if isinstance(v, str) else v)
    return out.astype("string")

# ---------- email ----------

EMAIL_RE = re.compile(
    r"^(?!.*\.\.)"                    # no consecutive dots anywhere
    r"[A-Za-z0-9._%+\-]+"             # local
    r"@"
    r"(?:[A-Za-z0-9\-]+\.)+"          # at least one dot in domain
    r"[A-Za-z]{2,}$"                  # TLD
)

def clean_email(s: pd.Series) -> pd.Series:
    t = s.astype(str).map(maybe_demojibake).str.strip()
    t = t.str.replace(r"\s*@\s*", "@", regex=True)  # remove spaces around @
    t = t.str.replace(" ", "", regex=False).str.lower()
    ok = t.str.match(EMAIL_RE)
    # also reject if starts/ends with dot in local or domain
    parts = t.str.split("@", n=1, expand=True)
    bad = (
        parts[0].str.startswith(".") | parts[0].str.endswith(".") |
        parts[1].str.startswith(".") | parts[1].str.endswith(".")
    )
    return t.where(ok & ~bad, pd.NA).astype("string")

# ---------- phone ----------
COUNTRY_DIAL = {
    "Sweden":"+46","Germany":"+49","France":"+33","Italy":"+39","USA":"+1",
    "Japan":"+81","Thailand":"+66","Brazil":"+55","Switzerland":"+41",
    "South Africa":"+27"
}

def clean_phone(phone: pd.Series, country: pd.Series) -> pd.Series:
    p = phone.astype(str).map(maybe_demojibake).str.strip()
    # normalize: remove spaces, (), -, ., / ; convert 00 -> +
    p = p.str.replace(r"\s|\(|\)|\-|\.|/", "", regex=True)
    p = p.str.replace(r"^00", "+", regex=True)
    p = p.str.replace(r"[^\d\+]", "", regex=True)
    # collapse to last +segment if multiple
    p = p.map(lambda v: v if v.count("+") <= 1 else "+" + re.sub(r"[^\d]", "", v.split("+")[-1]))

    def fmt(raw, cntry):
        if raw is None or pd.isna(raw) or raw == "":
            return pd.NA
        s = str(raw)
        code = COUNTRY_DIAL.get(str(cntry), None)
        digits = re.sub(r"\D", "", s)

        if code:
            cc = code.lstrip("+")
            if digits.startswith(cc):
                rest = digits[len(cc):]
            else:
                rest = digits
            if not rest:
                return pd.NA
            return f"(+{cc}) {rest}"
        else:
            m = re.match(r"^(\d{1,3})(\d+)$", digits)
            return f"(+{m.group(1)}) {m.group(2)}" if m else s

    cleaned = pd.Series([fmt(r, c) for r, c in zip(p, country if country is not None else [None]*len(p))],
                        index=p.index, dtype="string")

    ok = cleaned.str.replace(r"\D", "", regex=True).str.len().between(7, 15)
    return cleaned.where(ok, pd.NA)

# ---------- date ----------
def clean_date_iso(s: pd.Series) -> pd.Series:
    def parse_one(x):
        if x is None or (isinstance(x, float) and pd.isna(x)): return pd.NaT
        t = maybe_demojibake(str(x)).strip()
        t = re.sub(r"\s+", " ", t).replace("\u00a0", " ")
        for dayfirst in (False, True):
            dt = pd.to_datetime(t, errors="coerce", dayfirst=dayfirst, infer_datetime_format=True)
            if pd.notna(dt):
                if dt.year > datetime.now().year + 50: return pd.NaT
                return dt
        return pd.NaT
    parsed = s.map(parse_one)
    return parsed.dt.strftime("%Y-%m-%d")

# ---------- price ----------
CUR_HINTS = {"$":"USD","€":"EUR","usd":"USD","eur":"EUR","sek":"SEK","kr":"SEK"}
def _detect_currency(raw: str) -> str | None:
    low = raw.lower()
    for k, v in CUR_HINTS.items():
        if k in low or k in raw:
            return v
    return None

def _normalize_amount(raw: str) -> float | None:
    t = maybe_demojibake(str(raw)).replace("\u00a0"," ")
    t = re.sub(r"(?i)(usd|eur|sek|kr|\$|€)", "", t)
    t = t.replace(";", "").replace(" ", "")
    t = re.sub(r"[^0-9,\.]", "", t)
    if "," in t and "." in t:
        if t.rfind(",") > t.rfind("."):
            t = t.replace(".", "")
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    elif "," in t and "." not in t:
        parts = t.split(",")
        if len(parts[-1]) in (2, 3):
            t = t.replace(",", ".")
        else:
            t = t.replace(",", "")
    try:
        return float(t) if t != "" else None
    except:
        return None

def clean_total_spent(s: pd.Series) -> pd.Series:
    raw = s.astype(str)
    amounts = raw.map(_normalize_amount)
    curr = raw.map(lambda x: _detect_currency(str(x)))

    # 👇 FIXED: use .str.contains (twice), not .str_contains
    looks_eu = (
        raw.str.contains(r"\d{1,3}(?:\.\d{3})+,\d{2}") |
        raw.str.contains(r",\s*\d{2}\s*(€|eur)", case=False)
    )
    curr = curr.mask(curr.isna() & looks_eu.fillna(False), "EUR")

    # Format as "<amount> <CUR>"
    def fmt(a, c):
        if a is None:
            return pd.NA
        s = f"{a:.2f}"
        return f"{s} {c}" if pd.notna(c) else s

    return pd.Series([fmt(a, c) for a, c in zip(amounts, curr)], index=s.index, dtype="string")

# ---------- pipeline ----------
def main():
    df = pd.read_csv(INPUT_FILE, dtype=str, keep_default_na=True, na_values=["", "None", "NaN", "nan"])
    df.columns = df.columns.str.strip()
    df = tidy_whitespace(df)

    if "Name" in df: df["Name"] = clean_name(df["Name"])
    if "Email" in df: df["Email"] = clean_email(df["Email"])
    if set(["Phone","Country"]).issubset(df.columns):
        df["Phone"] = clean_phone(df["Phone"], df["Country"])
    elif "Phone" in df:
        df["Phone"] = clean_phone(df["Phone"], pd.Series([pd.NA]*len(df)))
    if "City" in df: df["City"] = clean_city(df["City"])
    if "Registration Date" in df: df["Registration Date"] = clean_date_iso(df["Registration Date"])
    if "Total Spent" in df: df["Total Spent"] = clean_total_spent(df["Total Spent"])

    df = df.drop_duplicates().reset_index(drop=True)
    df = tidy_whitespace(df)

    # Excel-friendly: prevent auto-format; use semicolon delimiter
    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        f.write("sep=;\n")
        df.to_csv(f, sep=";", index=False, quoting=csv.QUOTE_ALL)

    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        f.write("sep=;\n")
        df.to_csv(f, sep=";", index=False, quoting=csv.QUOTE_ALL)

    print(f"Saved -> {OUTPUT_FILE}")
    print(df.head(8).to_string(index=False))

if __name__ == "__main__":
    main()
