#!/usr/bin/env python3
"""
LUNA - Double-Entry Bookkeeper
===============================
FELIX 가 생성한 표준 트랜잭션(data/processed/transactions/)을 읽어
복식부기 분개장·계정별원장·부가세자료를 생성한다.

출력:
  data/processed/ledger/YYYY-MM_분개장.json
  data/processed/ledger/YYYY-MM_계정별원장.json
  data/processed/vat/YYYY-QN_부가세자료.json
  outputs/alerts/luna_errors_YYYYMMDD.json  (차/대 불일치)

CLI:
  python agents/luna.py --month 2026-03
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir


# ---------------------------------------------------------------------------
# Account Chart (계정과목)
# ---------------------------------------------------------------------------

ACCOUNTS = {
    # Revenue
    "제품매출": "revenue",
    "용역매출": "revenue",
    "숙박매출": "revenue",
    # COGS
    "원재료비": "cogs",
    "외주가공비": "cogs",
    "소모품비": "cogs",
    # SGA
    "임차료": "sga",
    "인건비": "sga",
    "통신비": "sga",
    "차량유지비": "sga",
    "수선비": "sga",
    "기타판관비": "sga",
    # Assets
    "현금": "asset",
    "보통예금": "asset",
    "미수금": "asset",
    "기계장치": "asset",
    "차량운반구": "asset",
    # Liabilities
    "미지급금": "liability",
    "단기차입금": "liability",
    "카드론": "liability",
    "기타채무": "liability",
    # Depreciation
    "감가상각비": "sga",
    "감가상각누계액": "contra_asset",
}

# 공방/펜션 공유비용 분담 비율
SHARED_SPLIT = {"workshop": 0.7, "pension": 0.3}

# 분기 매핑
MONTH_TO_QUARTER = {
    1: 1, 2: 1, 3: 1,
    4: 2, 5: 2, 6: 2,
    7: 3, 8: 3, 9: 3,
    10: 4, 11: 4, 12: 4,
}


# ---------------------------------------------------------------------------
# Journal Entry Builder
# ---------------------------------------------------------------------------

class JournalEntryBuilder:
    """분개 생성기. 하나의 월에 대한 분개 목록을 관리한다."""

    def __init__(self, month: str):
        self.month = month  # "YYYY-MM"
        self.entries: list[dict] = []
        self.errors: list[dict] = []
        self._date_seq: dict[str, int] = {}

    def _next_id(self, d: str) -> str:
        ds = d.replace("-", "") if d else "00000000"
        self._date_seq[ds] = self._date_seq.get(ds, 0) + 1
        return f"JE-{ds}-{self._date_seq[ds]:03d}"

    def add_entry(
        self,
        date_str: str,
        description: str,
        lines: list[dict],
        business_type: str,
    ) -> dict:
        """분개 추가. 차/대 밸런스를 검증한다."""
        debit_sum = sum(ln.get("debit", 0) for ln in lines)
        credit_sum = sum(ln.get("credit", 0) for ln in lines)
        balanced = debit_sum == credit_sum

        entry = {
            "entry_id": self._next_id(date_str),
            "date": date_str,
            "description": description,
            "lines": lines,
            "business_type": business_type,
            "balanced": balanced,
        }
        self.entries.append(entry)

        if not balanced:
            self.errors.append({
                "entry_id": entry["entry_id"],
                "date": date_str,
                "description": description,
                "debit_sum": debit_sum,
                "credit_sum": credit_sum,
                "diff": debit_sum - credit_sum,
            })

        return entry


# ---------------------------------------------------------------------------
# Transaction → Journal Entry Mapping
# ---------------------------------------------------------------------------

def map_transaction(txn: dict, builder: JournalEntryBuilder) -> None:
    """단일 트랜잭션을 분개로 변환하여 builder 에 추가한다."""
    d = txn.get("date") or "unknown"
    amt = txn.get("amount", 0)
    tax = txn.get("tax_amount", 0)
    btype = txn.get("business_type", "workshop")
    txn_type = txn.get("type", "expense")
    category = txn.get("category", "기타")
    counterparty = txn.get("counterparty") or "N/A"

    if amt == 0:
        return

    supply_amount = amt - tax if tax > 0 else amt

    # === INCOME ===
    if txn_type == "income":
        if btype == "pension":
            cr_account = "숙박매출"
        elif category == "CNC가공" or "가공" in category:
            cr_account = "제품매출"
        else:
            cr_account = "용역매출"

        lines = [
            {"account": "보통예금", "debit": amt, "credit": 0},
        ]
        if tax > 0:
            # 세금계산서 매출 → 공급가 + 부가세 분리
            lines.append({"account": cr_account, "debit": 0, "credit": supply_amount})
            lines.append({"account": "미지급금", "debit": 0, "credit": tax})  # 부가세 예수금
        else:
            lines.append({"account": cr_account, "debit": 0, "credit": amt})

        desc = f"{counterparty} {cr_account} ({category})"
        builder.add_entry(d, desc, lines, btype)
        return

    # === EXPENSE ===
    # 비용 계정 결정
    if category == "재료비":
        dr_account = "원재료비"
    elif category == "인건비":
        dr_account = "인건비"
    elif category == "임대료":
        dr_account = "임차료"
    elif category == "장비":
        dr_account = "수선비"
    elif "외주" in category:
        dr_account = "외주가공비"
    elif "통신" in category:
        dr_account = "통신비"
    elif "차량" in category:
        dr_account = "차량유지비"
    elif "소모" in category:
        dr_account = "소모품비"
    else:
        dr_account = "기타판관비"

    # 임대료는 공방/펜션 분할
    if dr_account == "임차료":
        ws_amt = int(amt * SHARED_SPLIT["workshop"])
        ps_amt = amt - ws_amt  # 나머지

        lines_ws = [
            {"account": "임차료", "debit": ws_amt, "credit": 0},
            {"account": "보통예금", "debit": 0, "credit": ws_amt},
        ]
        builder.add_entry(d, f"{counterparty} 임차료 (공방 70%)", lines_ws, "workshop")

        lines_ps = [
            {"account": "임차료", "debit": ps_amt, "credit": 0},
            {"account": "보통예금", "debit": 0, "credit": ps_amt},
        ]
        builder.add_entry(d, f"{counterparty} 임차료 (펜션 30%)", lines_ps, "pension")
        return

    # 매입세금계산서 있을 경우
    cr_account = "보통예금"
    lines = []
    if tax > 0:
        lines.append({"account": dr_account, "debit": supply_amount, "credit": 0})
        lines.append({"account": "미수금", "debit": tax, "credit": 0})  # 부가세 대급금
        lines.append({"account": cr_account, "debit": 0, "credit": amt})
    else:
        lines.append({"account": dr_account, "debit": amt, "credit": 0})
        lines.append({"account": cr_account, "debit": 0, "credit": amt})

    desc = f"{counterparty} {dr_account} ({category})"
    builder.add_entry(d, desc, lines, btype)


# ---------------------------------------------------------------------------
# Depreciation
# ---------------------------------------------------------------------------

def generate_depreciation_entries(builder: JournalEntryBuilder, month: str) -> None:
    """data/assets.json 을 읽어 월별 감가상각 분개를 생성한다."""
    root = get_project_root()
    assets_path = root / "data" / "assets.json"
    if not assets_path.exists():
        print("  [LUNA] assets.json 없음 → 감가상각 건너뜀")
        return

    data = load_json(assets_path)
    assets = data.get("assets", [])
    if not assets:
        return

    # 대상 월의 마지막 날
    year, mon = map(int, month.split("-"))
    if mon == 12:
        last_day = date(year + 1, 1, 1)
    else:
        last_day = date(year, mon + 1, 1)
    from datetime import timedelta
    last_day = last_day - timedelta(days=1)
    dep_date = last_day.strftime("%Y-%m-%d")

    for asset in assets:
        acq_date = datetime.strptime(asset["acquired_date"], "%Y-%m-%d").date()
        cost = asset["acquisition_cost"]
        life_years = asset["useful_life_years"]
        residual_rate = asset.get("residual_value_rate", 0.1)
        residual = int(cost * residual_rate)
        depreciable = cost - residual

        # 총 감가상각 월 수
        total_months = life_years * 12
        if total_months == 0:
            continue

        monthly_dep = int(depreciable / total_months)

        # 자산 취득 이전이면 건너뜀
        if date(year, mon, 1) < acq_date.replace(day=1):
            continue

        # 내용연수 초과 여부
        try:
            end_date = acq_date.replace(year=acq_date.year + life_years)
        except ValueError:
            # 윤년 2/29 등 예외 처리
            end_date = acq_date.replace(
                year=acq_date.year + life_years, day=acq_date.day - 1
            )
        if date(year, mon, 1) >= end_date.replace(day=1):
            continue

        lines = [
            {"account": "감가상각비", "debit": monthly_dep, "credit": 0},
            {"account": "감가상각누계액", "debit": 0, "credit": monthly_dep},
        ]
        desc = f"{asset['name']} 월 감가상각 (정액법)"
        builder.add_entry(dep_date, desc, lines, "workshop")

    print(f"  [LUNA] 감가상각 분개 {len(assets)}건 처리")


# ---------------------------------------------------------------------------
# Account Ledger (계정별원장)
# ---------------------------------------------------------------------------

def build_account_ledger(entries: list[dict]) -> dict:
    """분개 목록으로부터 계정별 원장을 생성한다."""
    ledger: dict[str, dict] = {}

    for entry in entries:
        for line in entry.get("lines", []):
            acct = line["account"]
            if acct not in ledger:
                ledger[acct] = {
                    "account": acct,
                    "type": ACCOUNTS.get(acct, "unknown"),
                    "total_debit": 0,
                    "total_credit": 0,
                    "balance": 0,
                    "entries": [],
                }
            ledger[acct]["total_debit"] += line.get("debit", 0)
            ledger[acct]["total_credit"] += line.get("credit", 0)
            ledger[acct]["entries"].append({
                "entry_id": entry["entry_id"],
                "date": entry["date"],
                "description": entry["description"],
                "debit": line.get("debit", 0),
                "credit": line.get("credit", 0),
            })

    # 잔액 계산 (자산·비용: 차변 - 대변, 부채·수익: 대변 - 차변)
    debit_normal = {"asset", "cogs", "sga", "contra_asset"}
    for acct, info in ledger.items():
        acct_type = info["type"]
        if acct_type in debit_normal:
            info["balance"] = info["total_debit"] - info["total_credit"]
        else:
            info["balance"] = info["total_credit"] - info["total_debit"]

    return ledger


# ---------------------------------------------------------------------------
# VAT Aggregation (부가세 자료)
# ---------------------------------------------------------------------------

def build_vat_data(
    transactions: list[dict],
    entries: list[dict],
    month: str,
) -> dict:
    """부가세 신고 기초자료를 생성한다."""
    year, mon = map(int, month.split("-"))
    quarter = MONTH_TO_QUARTER[mon]

    sales_tax = 0       # 매출세액
    purchase_tax = 0    # 매입세액
    sales_total = 0
    purchase_total = 0
    missing_invoices: list[dict] = []

    for txn in transactions:
        tax = txn.get("tax_amount", 0)
        amt = txn.get("amount", 0)

        if txn["type"] == "income":
            sales_total += amt
            sales_tax += tax
            # 세금계산서 없이 매출 잡힌 건
            if tax == 0 and amt >= 30000 and txn.get("business_type") == "workshop":
                missing_invoices.append({
                    "id": txn.get("id"),
                    "date": txn.get("date"),
                    "counterparty": txn.get("counterparty"),
                    "amount": amt,
                    "reason": "세금계산서 미발행 매출",
                })
        else:
            purchase_total += amt
            purchase_tax += tax
            # 매입 세금계산서 미수취
            if tax == 0 and amt >= 30000:
                missing_invoices.append({
                    "id": txn.get("id"),
                    "date": txn.get("date"),
                    "counterparty": txn.get("counterparty"),
                    "amount": amt,
                    "reason": "세금계산서 미수취 매입",
                })

    net_vat = sales_tax - purchase_tax

    return {
        "period": f"{year}-Q{quarter}",
        "month": month,
        "sales": {
            "total": sales_total,
            "tax": sales_tax,
            "count": sum(1 for t in transactions if t["type"] == "income"),
        },
        "purchases": {
            "total": purchase_total,
            "tax": purchase_tax,
            "count": sum(1 for t in transactions if t["type"] == "expense"),
        },
        "net_vat": net_vat,
        "vat_payable": max(net_vat, 0),
        "vat_refundable": abs(min(net_vat, 0)),
        "missing_invoices": missing_invoices,
        "missing_invoice_count": len(missing_invoices),
    }


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def run(month: str) -> None:
    """LUNA 메인 파이프라인."""
    root = get_project_root()
    txn_dir = root / "data" / "processed" / "transactions"
    txn_file = txn_dir / f"{month}_transactions.json"

    print(f"[LUNA] 분개 생성 시작: {month}")

    # 트랜잭션 로드
    transactions: list[dict] = []
    if txn_file.exists():
        transactions = load_json(txn_file)
        print(f"  → {txn_file.name}: {len(transactions)}건 로드")
    else:
        # 해당 월 파일이 없으면 디렉터리에서 전체 파일 탐색
        if txn_dir.exists():
            for f in sorted(txn_dir.glob("*.json")):
                data = load_json(f)
                if isinstance(data, list):
                    for t in data:
                        if t.get("date", "").startswith(month):
                            transactions.append(t)
            print(f"  → 디렉터리 스캔: {len(transactions)}건 (월 필터: {month})")

    if not transactions:
        print("[LUNA] 처리할 트랜잭션이 없습니다.")
        print("  → FELIX 를 먼저 실행하세요: python agents/felix.py --month " + month)
        return

    # 분개 생성
    builder = JournalEntryBuilder(month)

    for txn in transactions:
        map_transaction(txn, builder)

    # 감가상각
    generate_depreciation_entries(builder, month)

    # 분개장 저장
    ledger_dir = root / "data" / "processed" / "ledger"
    journal_path = ledger_dir / f"{month}_분개장.json"
    save_json(builder.entries, journal_path)
    print(f"[LUNA] 분개장 저장: {journal_path} ({len(builder.entries)}건)")

    # 계정별원장
    account_ledger = build_account_ledger(builder.entries)
    ledger_summary_path = ledger_dir / f"{month}_계정별원장.json"
    save_json(account_ledger, ledger_summary_path)
    print(f"[LUNA] 계정별원장 저장: {ledger_summary_path} ({len(account_ledger)}개 계정)")

    # 부가세 자료
    vat_data = build_vat_data(transactions, builder.entries, month)
    year, mon = map(int, month.split("-"))
    quarter = MONTH_TO_QUARTER[mon]
    vat_dir = root / "data" / "processed" / "vat"
    vat_path = vat_dir / f"{year}-Q{quarter}_부가세자료.json"
    save_json(vat_data, vat_path)
    print(f"[LUNA] 부가세자료 저장: {vat_path}")

    # 에러 알림
    if builder.errors:
        today_str = datetime.now().strftime("%Y%m%d")
        alert_path = root / "outputs" / "alerts" / f"luna_errors_{today_str}.json"
        save_json(builder.errors, alert_path)
        print(f"[LUNA] 차대 불일치 {len(builder.errors)}건 → {alert_path}")

    # 요약 통계
    total_debit = sum(
        ln.get("debit", 0)
        for e in builder.entries
        for ln in e.get("lines", [])
    )
    total_credit = sum(
        ln.get("credit", 0)
        for e in builder.entries
        for ln in e.get("lines", [])
    )
    balanced_count = sum(1 for e in builder.entries if e["balanced"])
    ws_count = sum(1 for e in builder.entries if e["business_type"] == "workshop")
    ps_count = sum(1 for e in builder.entries if e["business_type"] == "pension")

    print(f"\n[LUNA] === 요약 ===")
    print(f"  분개 총 {len(builder.entries)}건 (정상 {balanced_count}건, 에러 {len(builder.errors)}건)")
    print(f"  공방(workshop): {ws_count}건 / 펜션(pension): {ps_count}건")
    print(f"  차변 합계: {total_debit:,}원 / 대변 합계: {total_credit:,}원")
    if vat_data["missing_invoice_count"] > 0:
        print(f"  [주의] 세금계산서 누락 의심: {vat_data['missing_invoice_count']}건")
    print(f"  부가세 납부세액: {vat_data['net_vat']:,}원")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LUNA - 복식부기 분개 에이전트"
    )
    parser.add_argument(
        "--month",
        default=datetime.now().strftime("%Y-%m"),
        help="처리할 월 (예: 2026-03). 기본: 현재 월.",
    )
    args = parser.parse_args()
    run(args.month)


if __name__ == "__main__":
    main()
