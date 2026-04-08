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

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_URL           = "https://www.tijorifinance.com/results/quarterly-results/"
DATA_FILE          = Path("data/tijori_known.json")
STOCKS_CSV         = Path("indianStocks.csv")
FUZZY_THRESHOLD    = 0.75

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
WATCHLIST_RAW      = os.environ.get("WATCHLIST", "ALL")

# ── Load stock master from CSV ──────────────────────────────────────────────
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

# ── Fuzzy matching ──────────────────────────────────────────────────────────
def find_stock_info(company_name, master):
    query = company_name.lower().strip()
    if query in master:
        return master[query]
    matches = difflib.get_close_matches(query, list(master.keys()), n=1, cutoff=FUZZY_THRESHOLD)
    if matches:
        return master[matches[0]]
    # fallback substring match
    clean = query.rstrip(". ")
    for k, v in master.items():
        if clean in k or k in clean or (len(clean) >= 8 and k.startswith(clean[:8])):
            return v
    return {}

# ── Watchlist helpers ───────────────────────────────────────────────────────
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

# ── Scrape with Playwright (full table extraction) ─────────────────────────
def fetch_results():
    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        print("[INFO] Opening Tijori Finance...")
        page.goto(BASE_URL, wait_until="networkidle", timeout=60000)

        # Wait for dynamic content
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
                # --- Basic info ---
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

                # --- Full financial table ---
                # Get column headers from <thead>
                headers = []
                thead = item.query_selector("table.inner-table thead tr")
                if thead:
                    ths = thead.query_selector_all("th")
                    headers = [th.inner_text().strip() for th in ths]
                # fallback if no headers found
                if not headers:
                    headers = ["(In Cr.)", "YoY Growth", "QoQ Growth", "Latest", "Prev Quarter", "Year Ago"]

                # Get all data rows
                rows = item.query_selector_all("table.inner-table tbody tr")
                financial_rows = []
                for row in rows:
                    cols = row.query_selector_all("td")
                    if len(cols) < 3:
                        continue
                    row_data = [col.inner_text().strip() for col in cols]
                    # Pad to match header length
                    while len(row_data) < len(headers):
                        row_data.append("")
                    financial_rows.append(row_data)

                # Store structured data
                financials = {}
                for row in financial_rows:
                    metric = row[0]
                    financials[metric] = {headers[i]: row[i] for i in range(1, len(row))}

                # Detailed link
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
                        "financial_headers": headers,
                        "financial_rows": financial_rows,
                        "link": href,
                        "detail_link": detail_link,
                    })
            except Exception as e:
                print(f"[WARN] Error parsing item: {e}")
                continue

        browser.close()
    print(f"[INFO] Fetched {len(results)} results.")
    return results

# ── Persistence (known results) ────────────────────────────────────────────
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

# ── Telegram formatting and sending ────────────────────────────────────────
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

def format_financial_table(item):
    """Return a monospaced table string wrapped in a code block."""
    headers = item.get("financial_headers", [])
    rows = item.get("financial_rows", [])
    if not rows:
        return ""

    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(cell))

    # Build separator and header
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    header_line = "| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |"

    # Build data rows
    data_lines = []
    for row in rows:
        # Pad row if shorter than headers
        padded = row + [""] * (len(headers) - len(row))
        data_line = "| " + " | ".join(padded[i].ljust(col_widths[i]) for i in range(len(headers))) + " |"
        data_lines.append(data_line)

    table = header_line + "\n" + sep + "\n" + "\n".join(data_lines)
    return f"```\n{table}\n```"

# ── Main notification logic ─────────────────────────────────────────────────
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

        # Enrich with CSV data
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
        # Symbols
        sym_parts = []
        if item["nse"]:
            sym_parts.append(f"NSE: <code>{item['nse']}</code>")
        if item["bse"]:
            sym_parts.append(f"BSE: <code>{item['bse']}</code>")
        sym_line = "  |  ".join(sym_parts) if sym_parts else "Symbol: N/A"
        industry_line = f"🏭 {item['industry']}\n" if item["industry"] else ""
        mcap_pe = f"M.Cap: {item['mcap']}  |  PE: {item['pe']}"

        # Build message block
        block = (
            f"🏢 <b>{item['company']}</b>\n"
            f"{sym_line}\n"
            f"{industry_line}"
            f"📅 {item['date']}  |  {mcap_pe}\n"
        )

        # Add formatted table
        table = format_financial_table(item)
        if table:
            block += f"\n{table}\n"

        # Add link
        if item["detail_link"]:
            block += f'🔗 <a href="{item["detail_link"]}">View Detailed Financials</a>'
        elif item["link"]:
            block += f'🔗 <a href="{item["link"]}">View on Tijori</a>'

        lines.append(block.strip())

    send_in_batches(lines, header)

if __name__ == "__main__":
    notify()
