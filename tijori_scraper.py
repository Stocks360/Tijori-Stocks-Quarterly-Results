import os
import json
import csv
import difflib
import requests
from pathlib import Path
from datetime import datetime

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("[ERROR] Playwright not installed. Run: pip install playwright && playwright install chromium")
    exit(1)

# ── Config ────────────────────────────────────────────────────────────────
BASE_URL           = "https://www.tijorifinance.com/results/quarterly-results/"
DATA_FILE          = Path("data/tijori_known.json")
STOCKS_CSV         = Path("indianStocks.csv")
FUZZY_THRESHOLD    = 0.75

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_RAW      = os.environ.get("WATCHLIST", "ALL")

# ── Load stock master CSV ─────────────────────────────────────────────────
def load_stock_master():
    master = {}
    if not STOCKS_CSV.exists():
        print("[WARN] indianStocks.csv not found.")
        return master
    with STOCKS_CSV.open(encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            name     = row[0].strip()
            bse      = row[1].strip() if len(row) > 1 else ""
            nse      = row[2].strip() if len(row) > 2 else ""
            industry = row[4].strip() if len(row) > 4 else ""
            if name and name.lower() != "name":
                master[name.lower()] = {
                    "name": name, "bse": bse, "nse": nse, "industry": industry
                }
    print(f"[INFO] Loaded {len(master)} stocks from CSV")
    return master

# ── Fuzzy match ──────────────────────────────────────────────────────────
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

# ── Watchlist handling ───────────────────────────────────────────────────
def build_watchlist():
    raw = WATCHLIST_RAW.strip().upper()
    if not raw or raw == "ALL":
        return set()
    return set(x.strip().upper() for x in WATCHLIST_RAW.split(",") if x.strip())

def is_in_watchlist(stock_info, company_name, watchlist):
    if not watchlist:
        return True
    nse        = stock_info.get("nse", "").upper()
    bse        = str(stock_info.get("bse", "")).upper()
    name_upper = company_name.upper()
    for item in watchlist:
        if item in (nse, bse) or item in name_upper:
            return True
    return False

# ── Scrape with Playwright ───────────────────────────────────────────────
def fetch_results():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print("[INFO] Opening Tijori Finance...")
        page.goto(BASE_URL, wait_until="networkidle", timeout=60000)
        try:
            page.wait_for_selector("div.result_item", timeout=30000)
        except Exception:
            print("[WARN] Result items did not load in time.")
            browser.close()
            return results
        items = page.query_selector_all("div.result_item")
        print(f"[INFO] Found {len(items)} result items.")
        for item in items:
            try:
                name_el = item.query_selector("h6")
                company = name_el.inner_text().strip() if name_el else ""
                link_el = item.query_selector("div.company_date a")
                href = link_el.get_attribute("href") if link_el else ""
                if href and not href.startswith("http"):
                    href = "https://www.tijorifinance.com" + href
                date_el = item.query_selector("span.event_date")
                date_str = date_el.inner_text().strip() if date_el else ""
                mcap_el = item.query_selector("span.value")
                mcap = mcap_el.inner_text().strip() if mcap_el else ""
                pe_els = item.query_selector_all("span.value")
                pe = pe_els[1].inner_text().strip() if len(pe_els) > 1 else ""
                rows = item.query_selector_all("table.inner-table tbody tr")
                financials = {}
                for row in rows:
                    cols = row.query_selector_all("td")
                    if len(cols) < 4:
                        continue
                    metric = cols[0].inner_text().strip()
                    yoy = cols[1].inner_text().strip()
                    qoq = cols[2].inner_text().strip()
                    latest = cols[3].inner_text().strip()
                    financials[metric] = {"yoy": yoy, "qoq": qoq, "latest": latest}
                detail_el = item.query_selector("div.result_item__footer a")
                detail_link = detail_el.get_attribute("href") if detail_el else ""
                if detail_link and not detail_link.startswith("http"):
                    detail_link = "https://www.tijorifinance.com" + detail_link
                if company:
                    results.append({
                        "company": company,
                        "date": date_str,
                        "mcap": mcap,
                        "pe": pe,
                        "financials": financials,
                        "link": href,
                        "detail_link": detail_link,
                    })
            except Exception as e:
                print(f"[WARN] Error parsing item: {e}")
                continue
        browser.close()
    print(f"[INFO] Fetched {len(results)} results.")
    return results

# ── Persistence ──────────────────────────────────────────────────────────
def make_key(item):
    return f"{item['company']}|{item['date']}"

def load_known():
    if not DATA_FILE.exists():
        return set()
    with DATA_FILE.open() as f:
        return set(json.load(f))

def save_known(keys):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w") as f:
        json.dump(sorted(list(keys)), f, indent=2)

# ── Telegram helpers ─────────────────────────────────────────────────────
def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[WARN] Telegram credentials missing.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, data=payload, timeout=20)
        r.raise_for_status()
        print("[INFO] Telegram message sent.")
    except Exception as e:
        print(f"[ERROR] Failed to send Telegram: {e}")

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

def fmt_fin(financials, metric, emoji):
    f = financials.get(metric, {})
    if not f:
        return ""
    yoy = f.get("yoy", "-")
    qoq = f.get("qoq", "-")
    latest = f.get("latest", "-")
    return f"{emoji} <b>{metric}:</b> ₹{latest}Cr  YoY: {yoy}  QoQ: {qoq}"

# ── Main ─────────────────────────────────────────────────────────────────
def notify():
    now = datetime.now().strftime("%d %b %Y %I:%M %p IST")
    master = load_stock_master()
    watchlist = build_watchlist()
    current = fetch_results()
    known = load_known()

    new_watch = []
    new_keys = set(known)
    skipped = 0

    for item in current:
        k = make_key(item)
        if k in known:
            skipped += 1
            continue
        info = find_stock_info(item["company"], master)
        item["nse"] = info.get("nse", "")
        item["bse"] = info.get("bse", "")
        item["industry"] = info.get("industry", "")
        if is_in_watchlist(info, item["company"], watchlist):
            new_watch.append(item)
        new_keys.add(k)

    save_known(new_keys)

    wl_note = " (All Stocks)" if not watchlist else f" (Watchlist: {', '.join(sorted(watchlist))})"
    print(f"[{now}] New: {len(new_watch)} | Skipped: {skipped}{wl_note}")

    if not new_watch:
        print("[INFO] No new results to notify.")
        return

    header = (
        f"📊 <b>New Quarterly Results Published</b>{wl_note}\n"
        f"🕐 {now}\n"
        f"📌 {len(new_watch)} new result(s)"
    )

    lines = []
    for item in new_watch:
        sym_parts = []
        if item["nse"]:
            sym_parts.append(f"NSE: <code>{item['nse']}</code>")
        if item["bse"]:
            sym_parts.append(f"BSE: <code>{item['bse']}</code>")
        sym_line = "  |  ".join(sym_parts) if sym_parts else "Symbol: N/A"
        industry_line = f"🏭 {item['industry']}\n" if item["industry"] else ""
        mcap_pe = f"M.Cap: {item['mcap']}  |  PE: {item['pe']}"

        fin = item["financials"]
        sales_line = fmt_fin(fin, "Sales", "💰")
        op_line = fmt_fin(fin, "Operating Profit", "📈")
        net_line = fmt_fin(fin, "Net Profit", "🟢")

        line = (
            f"🏢 <b>{item['company']}</b>\n"
            f"{sym_line}\n"
            f"{industry_line}"
            f"📅 {item['date']}  |  {mcap_pe}\n"
        )
        if sales_line:
            line += f"{sales_line}\n"
        if op_line:
            line += f"{op_line}\n"
        if net_line:
            line += f"{net_line}\n"
        if item["detail_link"]:
            line += f'🔗 <a href="{item["detail_link"]}">View Detailed Financials</a>'
        elif item["link"]:
            line += f'🔗 <a href="{item["link"]}">View on Tijori</a>'

        lines.append(line.strip())

    send_in_batches(lines, header)

if __name__ == "__main__":
    notify()
