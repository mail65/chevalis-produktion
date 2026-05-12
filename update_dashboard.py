#!/usr/bin/env python3
"""
CHEVALIS Produktions-Dashboard Updater
Läuft alle 3 Tage via Cron → scrapt Sellerboard → berechnet Produktionsbedarf → pusht data.json
"""

import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────────────────────
# KONFIGURATION
# ─────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent

SELLERBOARD_EMAIL = "tobias.huebner@bergbad.com"
SELLERBOARD_PW    = "viqPak-fuzsyq-gadzi7"

# 2025 Monatliche Verkäufe pro SKU (Mai–Sept, Saison)
DATA_2025 = {
    "500ml Natur": {"05":282,"06":513,"07":570,"08":452,"09":165},
    "500ml Knobi":  {"05":68, "06":199,"07":269,"08":176,"09":48},
    "500ml Rauch":  {"05":57, "06":198,"07":276,"08":158,"09":31},
    "3L Natur":     {"05":137,"06":195,"07":359,"08":207,"09":33},
    "3L Rauch":     {"05":71, "06":166,"07":156,"08":49, "09":9},
    "5L Natur":     {"05":76, "06":185,"07":154,"08":72, "09":10},
    "5L Rauch":     {"05":23, "06":119,"07":110,"08":27, "09":3},
    "10L Natur":    {"05":16, "06":41, "07":43, "08":21, "09":1},
}

# Wachstumsfaktor 2026 pro SKU-Gruppe (basierend auf aktueller Velocity)
GROWTH = {
    "500ml Natur": 1.80,
    "500ml Knobi":  2.20,
    "500ml Rauch":  1.80,
    "3L Natur":     1.30,
    "3L Rauch":     1.30,
    "5L Natur":     1.30,
    "5L Rauch":     1.30,
    "10L Natur":    1.20,
}

# SKU-Mapping: Sellerboard SKU → unser Name
SKU_MAP = {
    "Insektenspray_500ml":       "500ml Natur",
    "Insektenspray_500ml_KNOBI": "500ml Knobi",
    "Insektenspray_500ml_RAUCH": "500ml Rauch",
    "Insektenspray_3l":          "3L Natur",
    "Insektenspray_3L_RAUCH":    "3L Rauch",
    "Insektenspray_5l":          "5L Natur",
    "Insektenspray_5L_RAUCH":    "5L Rauch",
    "Insektenspray_10l":         "10L Natur",
}

# Kanister pro 300L-Charge
KANISTER_PRO_CHARGE = {
    "3L Natur": 100, "3L Rauch": 100,
    "5L Natur": 60,  "5L Rauch": 60,
    "10L Natur": 30,
}

# Saison-Ende: 30. September
SAISON_ENDE = date(2026, 9, 30)


# ─────────────────────────────────────────────────────────────
# SELLERBOARD SCRAPING via Playwright
# ─────────────────────────────────────────────────────────────

SCRAPER_JS = """
const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  await page.goto('https://app.sellerboard.com/de/auth/login/', { waitUntil: 'networkidle' });
  await page.fill('input[type="email"], input[name="email"], textbox', process.env.SB_EMAIL);
  await page.fill('input[type="password"], input[name="password"]', process.env.SB_PW);
  await page.click('button:has-text("Weiter"), button[type="submit"]');
  await page.waitForURL('**/dashboard**', { timeout: 15000 });

  await page.goto('https://app.sellerboard.com/de/inventory?productsSearchQuery=chev&channel=FBA&showOOS=on&showGradeAndResell=new_grade_resell', { waitUntil: 'networkidle' });
  await page.waitForSelector('table', { timeout: 15000 });

  const rows = await page.evaluate(() => {
    const result = [];
    document.querySelectorAll('tbody tr').forEach(row => {
      const cells = row.querySelectorAll('td');
      if (cells.length < 8) return;
      const skuEl = row.querySelector('[data-sku], .sku-cell, td:nth-child(1)');
      const skuText = row.textContent;

      // Extract SKU from row
      const skuMatch = skuText.match(/Insektenspray_[\\w]+/);
      if (!skuMatch) return;

      const nums = [];
      cells.forEach(c => {
        const n = parseFloat(c.innerText.trim().replace(',', '.'));
        if (!isNaN(n)) nums.push(n);
      });

      result.push({
        sku: skuMatch[0],
        rawNums: nums,
        rawText: skuText.substring(0, 200)
      });
    });
    return result;
  });

  await browser.close();
  console.log(JSON.stringify(rows));
})();
"""


def scrape_sellerboard():
    """Scrapt Sellerboard Inventory und gibt {sku_name: {fba, inbound, velocity}} zurück"""
    import os, tempfile

    # Write temp JS file
    js_path = SCRIPT_DIR / "_scraper_tmp.js"
    js_path.write_text(SCRAPER_JS)

    env = os.environ.copy()
    env["SB_EMAIL"] = SELLERBOARD_EMAIL
    env["SB_PW"]    = SELLERBOARD_PW

    try:
        result = subprocess.run(
            ["node", str(js_path)],
            capture_output=True, text=True, timeout=60, env=env,
            cwd=str(SCRIPT_DIR)
        )
        if result.returncode != 0:
            print(f"Scraper error: {result.stderr}", file=sys.stderr)
            return None
        rows = json.loads(result.stdout)
        js_path.unlink(missing_ok=True)
        return rows
    except Exception as e:
        print(f"Scraper exception: {e}", file=sys.stderr)
        js_path.unlink(missing_ok=True)
        return None


def parse_inventory_from_snapshot(raw_rows):
    """Parst die Rohdaten aus dem Scraper"""
    inventory = {}
    for row in raw_rows:
        sku_raw = row.get("sku", "")
        name = SKU_MAP.get(sku_raw)
        if not name:
            continue
        nums = row.get("rawNums", [])
        # Typical column order: fba, reserved, velocity, days, inbound, ...
        if len(nums) >= 5:
            fba      = int(nums[0]) if nums[0] > 0 else 0
            velocity = nums[2] if nums[2] > 0 else 0.5
            inbound  = int(nums[4]) if nums[4] > 0 else 0
            inventory[name] = {"fba": fba, "inbound": inbound, "velocity": velocity}
    return inventory


# ─────────────────────────────────────────────────────────────
# FALLBACK: Letzte bekannte Werte (falls Scraping fehlschlägt)
# ─────────────────────────────────────────────────────────────

FALLBACK_INVENTORY = {
    "500ml Natur": {"fba": 415, "inbound": 797, "velocity": 20.77},
    "500ml Knobi":  {"fba": 763, "inbound": 38,  "velocity": 5.23},
    "500ml Rauch":  {"fba": 27,  "inbound": 212, "velocity": 4.42},
    "3L Natur":     {"fba": 254, "inbound": 147, "velocity": 3.42},
    "3L Rauch":     {"fba": 253, "inbound": 0,   "velocity": 1.66},
    "5L Natur":     {"fba": 125, "inbound": 0,   "velocity": 1.98},
    "5L Rauch":     {"fba": 221, "inbound": 0,   "velocity": 1.22},
    "10L Natur":    {"fba": 22,  "inbound": 50,  "velocity": 0.59},
}


# ─────────────────────────────────────────────────────────────
# BEDARFSBERECHNUNG
# ─────────────────────────────────────────────────────────────

def projected_daily(sku, target_date):
    """Prognostizierte Tages-Nachfrage für ein SKU an einem bestimmten Datum"""
    month = str(target_date.month).zfill(2)
    base = DATA_2025.get(sku, {}).get(month, 0)
    if base == 0:
        return 0.1  # minimal fallback
    days_in_month = 31 if target_date.month in [1,3,5,7,8,10,12] else 30 if target_date.month != 2 else 28
    return (base * GROWTH.get(sku, 1.3)) / days_in_month


def calculate_oos_date(sku, total_stock, start_date=None):
    """Berechnet das voraussichtliche OOS-Datum"""
    if start_date is None:
        start_date = date.today()
    stock = float(total_stock)
    d = start_date
    while d <= SAISON_ENDE:
        daily = projected_daily(sku, d)
        stock -= daily
        if stock <= 0:
            return d
        d += timedelta(days=1)
    return None  # kein OOS bis Saisonende


def calculate_season_demand(sku, from_date=None):
    """Gesamtbedarf von heute bis Saisonende"""
    if from_date is None:
        from_date = date.today()
    total = 0.0
    d = from_date
    while d <= SAISON_ENDE:
        total += projected_daily(sku, d)
        d += timedelta(days=1)
    return int(total)


def calculate_production_needed(sku, total_stock):
    """Wie viele Einheiten müssen noch produziert werden?"""
    demand = calculate_season_demand(sku)
    return max(0, demand - total_stock)


# ─────────────────────────────────────────────────────────────
# PRIORITY LOGIC
# ─────────────────────────────────────────────────────────────

def get_priority(oos_date, prod_needed):
    if prod_needed == 0:
        return "ok"
    if oos_date is None:
        return "low"
    days_until_oos = (oos_date - date.today()).days
    if days_until_oos <= 21:
        return "critical"
    elif days_until_oos <= 45:
        return "high"
    elif days_until_oos <= 75:
        return "medium"
    else:
        return "low"


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("Starte Sellerboard-Scraping...")
    raw = scrape_sellerboard()

    if raw:
        inventory = parse_inventory_from_snapshot(raw)
        source = "sellerboard_live"
        print(f"✅ Live-Daten: {len(inventory)} SKUs geladen")
    else:
        inventory = FALLBACK_INVENTORY
        source = "fallback_cached"
        print("⚠️ Fallback auf gespeicherte Werte")

    today = date.today()
    items = []

    for sku in ["500ml Natur","500ml Knobi","500ml Rauch","3L Natur","3L Rauch","5L Natur","5L Rauch","10L Natur"]:
        inv = inventory.get(sku, {"fba":0,"inbound":0,"velocity":0})
        total_stock = inv["fba"] + inv["inbound"]

        oos_date    = calculate_oos_date(sku, total_stock)
        prod_needed = calculate_production_needed(sku, total_stock)
        priority    = get_priority(oos_date, prod_needed)

        is_kanister = any(k in sku for k in ["3L","5L","10L"])
        chargen = None
        if is_kanister and prod_needed > 0:
            kpb = KANISTER_PRO_CHARGE.get(sku, 100)
            chargen = (prod_needed + kpb - 1) // kpb  # ceil

        items.append({
            "sku":          sku,
            "fba":          inv["fba"],
            "inbound":      inv["inbound"],
            "total_stock":  total_stock,
            "velocity":     inv["velocity"],
            "oos_date":     oos_date.isoformat() if oos_date else None,
            "prod_needed":  prod_needed,
            "chargen":      chargen,
            "priority":     priority,
            "is_kanister":  is_kanister,
        })

    # Sort: critical first
    priority_order = {"critical":0,"high":1,"medium":2,"low":3,"ok":4}
    items.sort(key=lambda x: priority_order.get(x["priority"], 5))

    data = {
        "updated_at": datetime.now().isoformat(),
        "updated_date": today.isoformat(),
        "source": source,
        "saison_ende": SAISON_ENDE.isoformat(),
        "items": items,
    }

    out_path = SCRIPT_DIR / "data.json"
    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    print(f"✅ data.json geschrieben ({len(items)} Einträge)")

    # Git commit & push
    try:
        subprocess.run(["git", "add", "data.json"], cwd=str(SCRIPT_DIR), check=True)
        msg = f"Update {today.isoformat()} ({source})"
        subprocess.run(["git", "commit", "-m", msg], cwd=str(SCRIPT_DIR), check=True)
        subprocess.run(["git", "push"], cwd=str(SCRIPT_DIR), check=True)
        print("✅ Gepusht zu GitHub")
    except subprocess.CalledProcessError as e:
        print(f"Git push failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
