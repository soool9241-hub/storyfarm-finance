"""Vercel Serverless Function - 대시보드 데이터 API"""
import json
import os
from http.server import BaseHTTPRequestHandler
from pathlib import Path

ROOT = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _load(path):
    if not path.exists():
        return None
    with open(str(path), "r", encoding="utf-8") as f:
        return json.load(f)


def _find(directory, month, keyword=""):
    if not directory.exists():
        return None
    for fpath in sorted(directory.glob("*.json")):
        if month in fpath.stem:
            if keyword and keyword not in fpath.stem:
                continue
            return _load(fpath)
    return None


def calc_interest(balance, rate):
    return int(balance * rate / 12)


def collect(month="2026-03"):
    PROCESSED = ROOT / "data" / "processed"
    DATA = ROOT / "data"

    ledger = _find(PROCESSED / "ledger", month, "계정별원장") or {}
    assets_data = _find(PROCESSED / "assets", month) or {}
    dep_list = assets_data.get("depreciation", [])
    debts_raw = _load(DATA / "debts.json") or {}
    debts = debts_raw.get("debts", []) if isinstance(debts_raw, dict) else debts_raw
    profit_data = _find(PROCESSED / "profit", month) or {}
    cashflow = _find(PROCESSED / "cashflow", month) or {}
    cost_data = _find(PROCESSED / "cost", month, "수주별원가") or {}
    sim_data = _load(PROCESSED / "debt" / "상환시뮬레이션.json") or {}

    revenue_items, expense_items = {}, {}
    total_revenue = total_cogs = total_sga = 0

    for acct_name, acct in ledger.items():
        acct_type = acct.get("type", "")
        debit = int(acct.get("total_debit", 0))
        credit = int(acct.get("total_credit", 0))
        if acct_type == "revenue" or "매출" in acct_name:
            revenue_items[acct_name] = credit
            total_revenue += credit
        elif acct_type == "cogs":
            expense_items[acct_name] = debit
            total_cogs += debit
        elif acct_type == "sga":
            expense_items[acct_name] = debit
            total_sga += debit

    total_interest = sum(
        calc_interest(int(d.get("current_balance", 0)), float(d.get("interest_rate", 0)))
        for d in debts if d.get("current_balance") and d.get("interest_rate")
    )

    gross_profit = total_revenue - total_cogs
    operating_income = gross_profit - total_sga
    net_income = operating_income - total_interest

    asset_cards = []
    total_book_value = 0
    for item in dep_list:
        bv = item.get("book_value", 0)
        total_book_value += bv
        asset_cards.append({
            "name": item.get("asset_name", ""),
            "acquisition_cost": item.get("acquisition_cost", 0),
            "book_value": bv,
            "accumulated_dep": item.get("accumulated_depreciation", 0),
            "monthly_dep": item.get("monthly_depreciation", 0),
            "life_pct": item.get("life_elapsed_pct", 0),
            "remaining_months": item.get("remaining_months", 0),
        })

    total_debt = sum(d.get("current_balance", 0) for d in debts)
    debt_cards = [{
        "name": d.get("name", ""),
        "balance": d.get("current_balance", 0),
        "rate_pct": round(d.get("interest_rate", 0) * 100, 1),
        "monthly_payment": d.get("monthly_payment", 0),
        "monthly_interest": calc_interest(d.get("current_balance", 0), d.get("interest_rate", 0)),
        "original": d.get("original_amount", 0),
        "paid_pct": round((1 - d.get("current_balance", 0) / d["original_amount"]) * 100, 1)
        if d.get("original_amount") else 0,
    } for d in debts]

    forecast_days = cashflow.get("daily_forecast", cashflow.get("forecast", []))[:30] if cashflow else []
    orders = cost_data.get("orders", [])[:10] if isinstance(cost_data, dict) else []
    pension = profit_data.get("pension_analysis", {})

    sim_summary = [
        {"name": sc.get("description", k), "total_months": sc.get("total_months", 0),
         "total_interest": sc.get("total_interest_paid", 0), "payoff_date": sc.get("payoff_date_est", "")}
        for k, sc in sim_data.get("scenarios", {}).items()
    ]

    return {
        "month": month,
        "summary": {
            "total_revenue": total_revenue, "total_cogs": total_cogs,
            "gross_profit": gross_profit, "total_sga": total_sga,
            "operating_income": operating_income, "interest_expense": total_interest,
            "net_income": net_income, "total_assets": total_book_value,
            "total_debt": total_debt,
            "debt_ratio": round(total_debt / total_book_value * 100, 1) if total_book_value > 0 else 0,
        },
        "revenue_breakdown": revenue_items,
        "expense_breakdown": expense_items,
        "assets": asset_cards,
        "debts": debt_cards,
        "cashflow_forecast": forecast_days,
        "danger_zones": cashflow.get("danger_zones", []) if cashflow else [],
        "orders": orders,
        "pension": pension,
        "repayment_sim": sim_summary,
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        data = collect("2026-03")
        body = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
