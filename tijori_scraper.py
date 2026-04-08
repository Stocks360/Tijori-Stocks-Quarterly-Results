import requests
import json
import csv
import difflib
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL      = "https://www.tijorifinance.com/results/quarterly-results/"
DATA_FILE     = Path("data/tijori_known.json")
STOCKS_CSV    = Path("indianStocks.csv")
FUZZY_THRESHOLD = 0.75

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_RAW      = os.environ.get("WATCHLIST", "ALL")


def load_stock_master():
    master = {}
    if not STOCKS_CSV.exists():
        print("[WARN] indianStocks.csv not found.")
        return master
    with STOCKS_CSV.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3: continue
            name = row[0].strip()
            if not name or name.lower() == "name": continue
            master[name.lower()] = {
                "name": name,
                "bse": row[1].strip() if len(row) > 1 else "",
                "nse": row[2].strip() if len(row) > 2 else "",
                "industry": row[4].strip() if len(row) > 4 else "",
            }
    print(f"[INFO] Loaded {len(master)} stocks")
    return master


def find_stock_info(company_name, master):
    query = company_name.lower().strip()
    if query in master:
        return master[query]
    matches = difflib.get_close_matches(query, list(master.keys()), n=1, cutoff=FUZZY_THRESHOLD)
    if matches:
        return master[matches[0]]
    clean = query.rstrip(". ")
    for k, v in master.items():
        if clean in k or k in clean or (len(clean) >= 8 and k.startswith(clean[:8])):
            return v
    return {}


def build_watchlist():
    raw = WATCHLIST_RAW.strip().upper()
    if not raw or raw == "ALL":
        return set()
    return set(x.strip().upper() for x in WATCHLIST_RAW.split(",") if x.strip())


def is_in_watchlist(stock_info, company_name, watchlist):
    if not watchlist:
        return True
    nse = stock_info.get("nse", "").upper()
    bse = str(stock_info.get("bse", "")).upper()
    name_upper = company_name.upper()
    for item in watchlist:
        if item in (nse, bse) or item in name_upper:
            return True
    return False


# ── Scrape Tijori Finance ─────────────────────────────────────────────────────
def fetch_quarterly_results():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    r = requests.get(BASE_URL, headers=headers, timeout=40)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    items = soup.find_all("div", class_="result_item")

    results = []
    for item in items:
        try:
            company_tag = item.find("h6")
            company = company_tag.get_text(strip=True) if company_tag else ""

            link_tag = item.find("a", href=True)
            company_url = "https://www.tijorifinance.com" + link_tag["href"] if link_tag else ""

            date_tag = item.find("span", class_="event_date")
            date_str = date_tag.get_text(strip=True) if date_tag else ""

            # Metrics
            metrics_divs = item.find_all("div", class_="metrics")
            mcap = pe = ""
            for div in metrics_divs:
                text = div.get_text(strip=True)
                if "M Cap" in text:
                    mcap = text.split(":", 1)[-1].strip()
                if "PE" in text:
                    pe = text.split(":", 1)[-1].strip()

            # Table Data
            table = item.find("table", class_="inner-table")
            financials = {}
            if table:
                rows = table.find_all("tr")[1:]  # skip header
                for row in rows:
                    cols = row.find_all("td")
                    if len(cols) < 5: continue
                    metric = cols[0].get_text(strip=True)
                    yoy = cols[1].get_text(strip=True)
                    qoq = cols[2].get_text(strip=True)
                    mar26 = cols[3].get_text(strip=True)
                    dec25 = cols[4].get_text(strip=True)
                    mar25 = cols[5].get_text(strip=True) if len(cols) > 5 else ""

                    financials[metric] = {
                        "yoy": yoy, "qoq": qoq,
                        "mar2026": mar26, "dec2025": dec25, "mar2025": mar25
                    }

            detail_link = ""
            footer_a = item.find("div", class_="result_item__footer")
            if footer_a:
                a = footer_a.find("a")
                if a:
                    detail_link = "https://www.tijorifinance.com" + a["href"] if a.get("href") else ""

            if company and date_str:
                results.append({
                    "company": company,
                    "date": date_str,
                    "mcap": mcap,
                    "pe": pe,
                    "financials": financials,
                    "detail_link": detail_link or company_url
                })
        except:
            continue

    print(f"[INFO] Fetched {len(results)} quarterly results")
    return results


def make_key(item):
    return f"{item['company']}|{item['date']}"


def load_known():
    if not DATA_FILE.exists():
        return set()
    with DATA_FILE.open(encoding="utf-8") as f:
        return set(json.load(f))


def save_known(keys):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(sorted(list(keys)), f, indent=2)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        requests.post(url, data=payload, timeout=20).raise_for_status()
    except: pass


def send_in_batches(lines, header):
    sep = "\n\n─────────────────\n\n"
    batch = header
    for line in lines:
        candidate = batch + (sep if batch != header else "\n\n") + line
        if len(candidate) > 3900:
            send_telegram(batch)
            batch = header + "\n\n" + line
        else:
            batch = candidate
    if batch:
        send_telegram(batch)


# ── Main ──────────────────────────────────────────────────────────────────────
def notify():
    now = datetime.now().strftime("%d %b %Y %I:%M %p IST")
    master = load_stock_master()
    watchlist = build_watchlist()
    current = fetch_quarterly_results()
    known = load_known()

    new_watch = []
    new_keys = set(known)

    for item in current:
        k = make_key(item)
        if k in known:
            continue

        info = find_stock_info(item["company"], master)
        item["nse"] = info.get("nse", "")
        item["bse"] = info.get("bse", "")
        item["industry"] = info.get("industry", "")

        if is_in_watchlist(info, item["company"], watchlist):
            new_watch.append(item)

        new_keys.add(k)

    save_known(new_keys)

    if not new_watch:
        print("[INFO] No new results today.")
        return

    header = f"📊 <b>New Quarterly Results</b> (All Stocks)\n🕐 {now}\n📌 {len(new_watch)} result(s) announced"

    lines = []
    for item in new_watch:
        sym = []
        if item.get("nse"): sym.append(f'<code>NSE: {item["nse"]}</code>')
        if item.get("bse"): sym.append(f'<code>BSE: {item["bse"]}</code>')
        sym_line = " | ".join(sym) if sym else ""

        mcap_pe = f"M Cap: {item['mcap']} | PE: {item['pe']}"

        fin = item["financials"]

        line = f"🏢 <b>{item['company']}</b>\n{sym_line}\n🏭 {item.get('industry','')}\n📅 {item['date']} | {mcap_pe}\n\n"

        # Full Table-like details
        for metric in ["Sales", "Operating Profit", "Net Profit"]:
            if metric in fin:
                d = fin[metric]
                yoy_color = "🟢" if "increase" in str(d.get("yoy","")) or "%" in str(d.get("yoy","")) else "➖"
                line += f"<b>{metric}</b>\n"
                line += f"   Mar 2026: ₹{d['mar2026']} Cr\n"
                line += f"   YoY: {yoy_color} {d['yoy']}   QoQ: {d['qoq']}\n\n"

        line += f'🔗 <a href="{item["detail_link"]}">View Detailed Financials →</a>'
        lines.append(line)

    send_in_batches(lines, header)


if __name__ == "__main__":
    notify()
