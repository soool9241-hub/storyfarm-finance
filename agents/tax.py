"""
storyfarm-finance / agents / tax.py
TAX - 세무 준비 에이전트

부가가치세 집계, 종합소득세 추정, 누락 세금계산서 탐지,
세무 일정 알림을 수행한다.

CLI:
    python agents/tax.py --quarter 2026-Q1 --prepare
    python agents/tax.py --check-missing
    python agents/tax.py --income-tax --year 2025
    python agents/tax.py --calendar
"""

import argparse
import glob as glob_mod
import sys
import os
from datetime import datetime, date
from typing import List, Dict, Any, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir
from tools.calculator import calc_vat, calc_interest

# ──────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────
TODAY = date.today()
TODAY_STR = TODAY.strftime("%Y-%m-%d")
ROOT = get_project_root()

LEDGER_DIR = ROOT / "data" / "processed" / "ledger"
TRANSACTIONS_DIR = ROOT / "data" / "processed" / "transactions"

# 비공제 항목 카테고리
NON_DEDUCTIBLE_CATEGORIES = {"접대비", "entertainment"}

# 공제 가능 비용 카테고리
DEDUCTIBLE_EXPENSE_CATEGORIES = {
    "재료비", "인건비", "임차료", "감가상각비", "수도광열비",
    "통신비", "소모품비", "운반비", "수선비", "보험료",
    "차량유지비", "교육훈련비", "도서인쇄비", "광고선전비",
    "여비교통비", "사무용품비", "세금과공과",
    "material", "labor", "rent", "depreciation", "utilities",
    "communication", "supplies", "transportation",
}

# 한국 종합소득세 세율 구간
TAX_BRACKETS = [
    (14_000_000, 0.06, 0),
    (50_000_000, 0.15, 1_260_000),
    (88_000_000, 0.24, 5_760_000),
    (150_000_000, 0.35, 15_440_000),
    (300_000_000, 0.38, 19_940_000),
    (500_000_000, 0.40, 25_940_000),
    (1_000_000_000, 0.42, 35_940_000),
    (float("inf"), 0.45, 65_940_000),
]

# 세무 일정
TAX_CALENDAR = [
    {"name": "부가가치세 확정신고 (1기)", "date": "01-25", "type": "VAT"},
    {"name": "부가가치세 확정신고 (2기)", "date": "07-25", "type": "VAT"},
    {"name": "종합소득세 확정신고", "date": "05-31", "type": "income_tax"},
    {"name": "종합소득세 중간예납", "date": "11-30", "type": "interim"},
]


# ──────────────────────────────────────────────────────────────
# 데이터 로드 헬퍼
# ──────────────────────────────────────────────────────────────
def _load_all_json_in_dir(directory) -> List[Dict]:
    """디렉터리 안의 모든 JSON 파일을 로드하여 리스트로 반환."""
    results = []
    dir_path = str(directory)
    if not os.path.isdir(dir_path):
        return results
    for filepath in sorted(glob_mod.glob(os.path.join(dir_path, "*.json"))):
        try:
            data = load_json(filepath)
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                # entries 키가 있으면 리스트로 풀기
                if "entries" in data:
                    results.extend(data["entries"])
                elif "transactions" in data:
                    results.extend(data["transactions"])
                elif "items" in data:
                    results.extend(data["items"])
                else:
                    results.append(data)
        except Exception as e:
            print(f"[TAX] 경고: {filepath} 로드 실패 - {e}")
    return results


def load_ledger_entries(quarter: Optional[str] = None) -> List[Dict]:
    """LUNA 분개 데이터를 로드한다. quarter가 지정되면 해당 분기만 필터."""
    entries = _load_all_json_in_dir(LEDGER_DIR)
    if quarter and entries:
        year, q = _parse_quarter(quarter)
        if year and q:
            start_month, end_month = _quarter_months(q)
            entries = [
                e for e in entries
                if _entry_in_period(e, year, start_month, end_month)
            ]
    return entries


def load_transactions(quarter: Optional[str] = None) -> List[Dict]:
    """FELIX 거래 데이터를 로드한다."""
    txns = _load_all_json_in_dir(TRANSACTIONS_DIR)
    if quarter and txns:
        year, q = _parse_quarter(quarter)
        if year and q:
            start_month, end_month = _quarter_months(q)
            txns = [
                t for t in txns
                if _entry_in_period(t, year, start_month, end_month)
            ]
    return txns


def _parse_quarter(quarter_str: str):
    """'2026-Q1' -> (2026, 1)"""
    try:
        parts = quarter_str.split("-Q")
        return int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        print(f"[TAX] 분기 형식 오류: {quarter_str} (예: 2026-Q1)")
        return None, None


def _quarter_months(q: int):
    """분기번호 -> (시작월, 종료월)"""
    return {
        1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12),
    }.get(q, (1, 3))


def _entry_in_period(entry: Dict, year: int, start_month: int, end_month: int) -> bool:
    """항목의 날짜가 해당 기간에 속하는지 확인."""
    date_str = entry.get("date") or entry.get("transaction_date") or entry.get("거래일") or ""
    if not date_str:
        return True  # 날짜 없으면 포함
    try:
        d = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
        return d.year == year and start_month <= d.month <= end_month
    except ValueError:
        return True


# ──────────────────────────────────────────────────────────────
# 1. 부가가치세 자동 집계
# ──────────────────────────────────────────────────────────────
def aggregate_vat(quarter: str) -> Dict[str, Any]:
    """
    부가세 집계를 수행한다.
    - 매출세액: income 거래의 tax_amount 합
    - 매입세액: expense 거래의 tax_amount 합
    - 접대비 등 비공제 자동 분류
    - 순 부가세 = 매출세액 - 매입세액
    """
    ledger = load_ledger_entries(quarter)
    transactions = load_transactions(quarter)
    all_items = ledger + transactions

    if not all_items:
        print(f"[TAX] {quarter} 기간 데이터가 없습니다. 빈 집계를 생성합니다.")

    sales_tax = 0        # 매출세액
    purchase_tax = 0     # 매입세액
    non_deductible = 0   # 불공제 매입세액
    sales_details = []
    purchase_details = []
    non_deductible_details = []
    missing_invoice = []  # 세금계산서 누락 의심 매출

    for item in all_items:
        tx_type = (item.get("type") or item.get("transaction_type")
                   or item.get("유형") or "").lower()
        category = (item.get("category") or item.get("계정과목") or "").strip()
        tax_amount = _to_int(item.get("tax_amount") or item.get("세액") or 0)
        amount = _to_int(item.get("amount") or item.get("금액") or 0)
        has_invoice = item.get("has_tax_invoice", item.get("세금계산서", None))
        description = item.get("description") or item.get("적요") or item.get("name") or ""

        detail = {
            "date": item.get("date") or item.get("transaction_date") or item.get("거래일") or "",
            "description": description,
            "amount": amount,
            "tax_amount": tax_amount,
            "category": category,
        }

        if tx_type in ("income", "revenue", "매출", "sale", "sales"):
            sales_tax += tax_amount
            sales_details.append(detail)
            # 세금계산서 누락 확인
            if amount > 0 and not has_invoice and has_invoice is not True:
                missing_invoice.append({
                    **detail,
                    "alert": "매출 세금계산서 미발행 의심",
                })
        elif tx_type in ("expense", "cost", "매입", "purchase", "지출"):
            if category in NON_DEDUCTIBLE_CATEGORIES:
                non_deductible += tax_amount
                non_deductible_details.append({**detail, "reason": "접대비 불공제"})
            else:
                purchase_tax += tax_amount
                purchase_details.append(detail)

    net_vat = sales_tax - purchase_tax

    result = {
        "quarter": quarter,
        "generated_at": TODAY_STR,
        "summary": {
            "sales_tax": sales_tax,
            "sales_tax_label": "매출세액",
            "purchase_tax": purchase_tax,
            "purchase_tax_label": "매입세액",
            "non_deductible_tax": non_deductible,
            "non_deductible_label": "불공제 매입세액",
            "net_vat": net_vat,
            "net_vat_label": "납부(환급)세액",
            "vat_direction": "납부" if net_vat >= 0 else "환급",
        },
        "sales_count": len(sales_details),
        "purchase_count": len(purchase_details),
        "non_deductible_count": len(non_deductible_details),
        "sales_details": sales_details[:50],  # 상세는 최대 50건
        "purchase_details": purchase_details[:50],
        "non_deductible_details": non_deductible_details,
        "missing_invoice_alerts": missing_invoice,
    }

    # 저장
    out_path = ROOT / "data" / "processed" / "vat" / f"{quarter}_부가세집계.json"
    save_json(result, out_path)
    print(f"[TAX] 부가세 집계 저장: {out_path}")
    print(f"      매출세액: {sales_tax:,}원 | 매입세액: {purchase_tax:,}원 | "
          f"납부세액: {net_vat:,}원")

    return result


def _to_int(val) -> int:
    """값을 정수로 변환. 실패 시 0."""
    try:
        return int(float(str(val).replace(",", "")))
    except (ValueError, TypeError):
        return 0


# ──────────────────────────────────────────────────────────────
# 2. 종합소득세 추정
# ──────────────────────────────────────────────────────────────
def estimate_income_tax(year: int = None) -> Dict[str, Any]:
    """
    연간 종합소득세를 추정한다.
    사업소득 = 총수입 - 필요경비
    세율 구간에 따라 산출세액 계산.
    """
    if year is None:
        year = TODAY.year - 1  # 전년도 기준

    # 4분기 전체 데이터 로드
    all_items = []
    for q in range(1, 5):
        quarter = f"{year}-Q{q}"
        ledger = load_ledger_entries(quarter)
        transactions = load_transactions(quarter)
        all_items.extend(ledger + transactions)

    total_revenue = 0
    expense_breakdown = {}
    total_expenses = 0

    for item in all_items:
        tx_type = (item.get("type") or item.get("transaction_type")
                   or item.get("유형") or "").lower()
        category = (item.get("category") or item.get("계정과목") or "").strip()
        amount = _to_int(item.get("amount") or item.get("금액") or 0)

        if tx_type in ("income", "revenue", "매출", "sale", "sales"):
            total_revenue += amount
        elif tx_type in ("expense", "cost", "매입", "purchase", "지출"):
            # 접대비 한도 등은 간이 처리
            cat_key = category if category else "기타비용"
            is_deductible = (
                cat_key in DEDUCTIBLE_EXPENSE_CATEGORIES
                or cat_key not in NON_DEDUCTIBLE_CATEGORIES
            )
            if is_deductible:
                expense_breakdown[cat_key] = expense_breakdown.get(cat_key, 0) + amount
                total_expenses += amount

    taxable_income = max(0, total_revenue - total_expenses)

    # 세액 계산
    tax_result = _calculate_tax(taxable_income)

    # 절세 제안
    suggestions = _generate_tax_suggestions(taxable_income, total_revenue, expense_breakdown)

    result = {
        "year": year,
        "generated_at": TODAY_STR,
        "income_summary": {
            "total_revenue": total_revenue,
            "total_revenue_label": "총수입금액",
            "total_expenses": total_expenses,
            "total_expenses_label": "필요경비",
            "taxable_income": taxable_income,
            "taxable_income_label": "사업소득금액 (과세표준)",
        },
        "expense_breakdown": {
            k: {"amount": v, "label": k}
            for k, v in sorted(expense_breakdown.items(), key=lambda x: -x[1])
        },
        "tax_calculation": tax_result,
        "tax_saving_suggestions": suggestions,
    }

    out_path = ROOT / "data" / "processed" / "tax" / f"{year}_종소세예상.json"
    save_json(result, out_path)
    print(f"[TAX] 종소세 추정 저장: {out_path}")
    print(f"      수입: {total_revenue:,}원 | 경비: {total_expenses:,}원 | "
          f"과세표준: {taxable_income:,}원")
    print(f"      산출세액: {tax_result['tax']:,}원 (실효세율 {tax_result['effective_rate']}%)")

    return result


def _calculate_tax(taxable_income: int) -> Dict[str, Any]:
    """종합소득세 산출세액을 계산한다."""
    tax = 0
    applied_bracket = ""

    for upper, rate, cumulative in TAX_BRACKETS:
        if taxable_income <= upper:
            prev_upper = 0
            for u, r, c in TAX_BRACKETS:
                if u == upper:
                    break
                prev_upper = u
            tax = cumulative + int((taxable_income - prev_upper) * rate)
            applied_bracket = f"{prev_upper / 1_000_000:.0f}M ~ {upper / 1_000_000:.0f}M ({rate * 100:.0f}%)"
            if upper == float("inf"):
                applied_bracket = f"{prev_upper / 1_000_000:.0f}M 초과 ({rate * 100:.0f}%)"
            break

    effective_rate = round(tax / taxable_income * 100, 2) if taxable_income > 0 else 0.0

    # 지방소득세 (산출세액의 10%)
    local_tax = int(tax * 0.1)

    return {
        "taxable_income": taxable_income,
        "tax": tax,
        "tax_label": "산출세액",
        "local_income_tax": local_tax,
        "local_tax_label": "지방소득세 (10%)",
        "total_tax": tax + local_tax,
        "total_tax_label": "총 예상 세금",
        "effective_rate": effective_rate,
        "applied_bracket": applied_bracket,
    }


def _generate_tax_suggestions(taxable_income: int, revenue: int,
                               expenses: Dict) -> List[Dict[str, str]]:
    """절세 제안을 생성한다."""
    suggestions = []

    # 노란우산공제
    if taxable_income > 0:
        max_deduction = 5_000_000
        if taxable_income <= 40_000_000:
            max_deduction = 5_000_000
        elif taxable_income <= 100_000_000:
            max_deduction = 3_000_000
        else:
            max_deduction = 2_000_000
        suggestions.append({
            "item": "노란우산공제",
            "description": f"소기업/소상공인 공제부금. 연 최대 {max_deduction:,}원 소득공제 가능",
            "potential_saving": f"최대 {max_deduction:,}원 소득공제",
            "priority": "high",
        })

    # 업무용차량
    vehicle_cost = expenses.get("차량유지비", 0)
    if vehicle_cost == 0:
        suggestions.append({
            "item": "업무용차량 비용처리",
            "description": "업무용차량 감가상각비, 유류비, 보험료 등 연 1,500만원 한도 경비 인정",
            "potential_saving": "연 최대 15,000,000원 경비처리 가능",
            "priority": "medium",
        })

    # 교육훈련비
    training_cost = expenses.get("교육훈련비", 0)
    if training_cost == 0:
        suggestions.append({
            "item": "교육훈련비",
            "description": "직원 교육훈련비 전액 경비 인정. 세미나, 자격증, 온라인 교육 포함",
            "potential_saving": "교육비 전액 경비처리",
            "priority": "medium",
        })

    # 퇴직연금(IRP)
    suggestions.append({
        "item": "개인형퇴직연금(IRP)",
        "description": "연 최대 900만원 세액공제 대상 (연금저축 포함)",
        "potential_saving": "최대 1,485,000원 세액공제 (16.5%)",
        "priority": "high",
    })

    # 기장세액공제
    if revenue <= 75_000_000:
        suggestions.append({
            "item": "기장세액공제",
            "description": "간편장부 대상자가 복식부기 기장 시 산출세액 20% 공제 (100만원 한도)",
            "potential_saving": "산출세액 20% 공제",
            "priority": "medium",
        })

    return suggestions


# ──────────────────────────────────────────────────────────────
# 3. 세금계산서 누락 탐지
# ──────────────────────────────────────────────────────────────
def check_missing_invoices(quarter: Optional[str] = None) -> Dict[str, Any]:
    """
    카드 결제 건 중 세금계산서 미수취 건을 탐지한다.
    10만원 초과 결제는 세금계산서 수취 권장.
    """
    transactions = load_transactions(quarter)
    ledger = load_ledger_entries(quarter)
    all_items = transactions + ledger

    missing = []
    threshold = 100_000

    for item in all_items:
        tx_type = (item.get("type") or item.get("transaction_type")
                   or item.get("유형") or "").lower()
        amount = _to_int(item.get("amount") or item.get("금액") or 0)
        payment_method = (item.get("payment_method") or item.get("결제수단") or "").lower()
        has_invoice = item.get("has_tax_invoice", item.get("세금계산서", None))
        description = item.get("description") or item.get("적요") or item.get("name") or ""
        category = item.get("category") or item.get("계정과목") or ""

        # 비용 거래 중 카드 결제 10만원 초과 & 세금계산서 없음
        is_expense = tx_type in ("expense", "cost", "매입", "purchase", "지출")
        is_card = "card" in payment_method or "카드" in payment_method
        if (is_expense and amount > threshold and is_card and not has_invoice):
            estimated_vat = calc_vat(amount)
            missing.append({
                "date": (item.get("date") or item.get("transaction_date")
                         or item.get("거래일") or ""),
                "description": description,
                "amount": amount,
                "estimated_vat": estimated_vat,
                "payment_method": payment_method,
                "category": category,
                "suggestion": "세금계산서 수취 요청 권장",
            })

    result = {
        "generated_at": TODAY_STR,
        "quarter": quarter or "전체",
        "threshold": threshold,
        "missing_count": len(missing),
        "total_missing_amount": sum(m["amount"] for m in missing),
        "total_estimated_vat_loss": sum(m["estimated_vat"] for m in missing),
        "items": missing,
    }

    # 알림 파일 저장
    if missing:
        alert_path = (ROOT / "outputs" / "alerts"
                      / f"tax_missing_{TODAY.strftime('%Y%m%d')}.json")
        save_json(result, alert_path)
        print(f"[TAX] 세금계산서 누락 알림 저장: {alert_path}")
    else:
        print("[TAX] 세금계산서 누락 건 없음 (또는 데이터 없음)")

    print(f"[TAX] 누락 의심 {len(missing)}건, "
          f"예상 매입세액 손실: {result['total_estimated_vat_loss']:,}원")

    return result


# ──────────────────────────────────────────────────────────────
# 4. 세무 일정 알림
# ──────────────────────────────────────────────────────────────
def check_tax_calendar() -> List[Dict[str, Any]]:
    """14일 이내 세무 일정을 확인한다."""
    reminders = []
    current_year = TODAY.year

    for event in TAX_CALENDAR:
        # 올해와 내년 날짜 확인
        for y in [current_year, current_year + 1]:
            try:
                event_date = datetime.strptime(f"{y}-{event['date']}", "%Y-%m-%d").date()
            except ValueError:
                continue

            delta = (event_date - TODAY).days
            if 0 <= delta <= 14:
                level = "urgent" if delta <= 3 else ("warning" if delta <= 7 else "reminder")
                reminders.append({
                    "event": event["name"],
                    "event_type": event["type"],
                    "date": event_date.strftime("%Y-%m-%d"),
                    "days_remaining": delta,
                    "level": level,
                    "message": f"{event['name']} D-{delta}일 ({event_date.strftime('%Y-%m-%d')})",
                })

    if reminders:
        for r in reminders:
            print(f"[TAX] {r['message']}")
    else:
        print("[TAX] 14일 이내 세무 일정 없음")

    return reminders


# ──────────────────────────────────────────────────────────────
# 보고서 생성
# ──────────────────────────────────────────────────────────────
def generate_report(quarter: str, vat: Dict | None, income_tax: Dict | None,
                    missing: Dict | None, calendar: List[Dict]) -> str:
    """텍스트 보고서를 생성한다."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  세무 준비 보고서 - {quarter}")
    lines.append(f"  생성일: {TODAY_STR}")
    lines.append("=" * 60)
    lines.append("")

    # 1. 부가세 집계
    if vat:
        s = vat["summary"]
        lines.append("[1] 부가가치세 집계")
        lines.append("-" * 40)
        lines.append(f"  대상 분기:    {vat['quarter']}")
        lines.append(f"  매출세액:     {s['sales_tax']:>12,}원")
        lines.append(f"  매입세액:     {s['purchase_tax']:>12,}원")
        lines.append(f"  불공제세액:   {s['non_deductible_tax']:>12,}원")
        lines.append(f"  {s['vat_direction']}세액:     {abs(s['net_vat']):>12,}원 ({s['vat_direction']})")
        lines.append(f"  매출 건수: {vat['sales_count']}건 | 매입 건수: {vat['purchase_count']}건")
        if vat.get("missing_invoice_alerts"):
            lines.append(f"  *** 세금계산서 미발행 의심: {len(vat['missing_invoice_alerts'])}건 ***")
        lines.append("")

    # 2. 종소세 추정
    if income_tax:
        inc = income_tax["income_summary"]
        tax = income_tax["tax_calculation"]
        lines.append("[2] 종합소득세 추정")
        lines.append("-" * 40)
        lines.append(f"  대상 연도:    {income_tax['year']}년")
        lines.append(f"  총수입금액:   {inc['total_revenue']:>12,}원")
        lines.append(f"  필요경비:     {inc['total_expenses']:>12,}원")
        lines.append(f"  과세표준:     {inc['taxable_income']:>12,}원")
        lines.append(f"  산출세액:     {tax['tax']:>12,}원")
        lines.append(f"  지방소득세:   {tax['local_income_tax']:>12,}원")
        lines.append(f"  총 예상세금:  {tax['total_tax']:>12,}원")
        lines.append(f"  실효세율:     {tax['effective_rate']}%")
        lines.append(f"  적용구간:     {tax['applied_bracket']}")
        lines.append("")

        if income_tax.get("expense_breakdown"):
            lines.append("  [경비 내역]")
            for cat, info in income_tax["expense_breakdown"].items():
                lines.append(f"    {cat}: {info['amount']:>12,}원")
            lines.append("")

        if income_tax.get("tax_saving_suggestions"):
            lines.append("  [절세 제안]")
            for sg in income_tax["tax_saving_suggestions"]:
                lines.append(f"    [{sg['priority'].upper()}] {sg['item']}")
                lines.append(f"      {sg['description']}")
                lines.append(f"      효과: {sg['potential_saving']}")
            lines.append("")

    # 3. 세금계산서 누락
    if missing and missing.get("missing_count", 0) > 0:
        lines.append("[3] 세금계산서 누락 탐지")
        lines.append("-" * 40)
        lines.append(f"  누락 의심: {missing['missing_count']}건")
        lines.append(f"  총 금액:   {missing['total_missing_amount']:>12,}원")
        lines.append(f"  예상 매입세액 손실: {missing['total_estimated_vat_loss']:>12,}원")
        for m in missing["items"][:10]:
            lines.append(f"    {m['date']} | {m['description']} | {m['amount']:,}원")
        lines.append("")

    # 4. 세무 일정
    if calendar:
        lines.append("[4] 세무 일정 알림")
        lines.append("-" * 40)
        for c in calendar:
            level_kr = {"urgent": "[긴급]", "warning": "[주의]", "reminder": "[알림]"
                       }.get(c["level"], "[알림]")
            lines.append(f"  {level_kr} {c['message']}")
        lines.append("")

    lines.append("=" * 60)
    lines.append("  TAX Agent 자동 생성 보고서")
    lines.append("=" * 60)

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="TAX - 세무 준비 에이전트",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  python agents/tax.py --quarter 2026-Q1 --prepare\n"
            "  python agents/tax.py --check-missing\n"
            "  python agents/tax.py --income-tax --year 2025\n"
            "  python agents/tax.py --calendar\n"
            "  python agents/tax.py --all --quarter 2026-Q1"
        ),
    )
    parser.add_argument("--quarter", type=str, default=None,
                        help="대상 분기 (예: 2026-Q1)")
    parser.add_argument("--year", type=int, default=None,
                        help="소득세 대상 연도 (예: 2025)")
    parser.add_argument("--prepare", action="store_true",
                        help="부가세 집계 실행")
    parser.add_argument("--income-tax", action="store_true",
                        help="종합소득세 추정 실행")
    parser.add_argument("--check-missing", action="store_true",
                        help="세금계산서 누락 탐지")
    parser.add_argument("--calendar", action="store_true",
                        help="세무 일정 알림 확인")
    parser.add_argument("--all", action="store_true",
                        help="모든 분석 실행")

    args = parser.parse_args()

    # 인자 없으면 --all
    if not (args.prepare or args.income_tax or args.check_missing
            or args.calendar or args.all):
        args.all = True

    # 분기 기본값 설정
    quarter = args.quarter
    if not quarter:
        q = (TODAY.month - 1) // 3 + 1
        quarter = f"{TODAY.year}-Q{q}"
        print(f"[TAX] 분기 미지정. 현재 분기 사용: {quarter}")

    year = args.year or TODAY.year - 1

    vat_result = None
    income_result = None
    missing_result = None
    calendar_result = []
    saved_files = []

    # 1. 부가세 집계
    if args.prepare or args.all:
        print(f"\n[TAX] === 부가세 집계: {quarter} ===")
        vat_result = aggregate_vat(quarter)
        saved_files.append(
            str(ROOT / "data" / "processed" / "vat" / f"{quarter}_부가세집계.json")
        )

    # 2. 종합소득세 추정
    if args.income_tax or args.all:
        print(f"\n[TAX] === 종합소득세 추정: {year}년 ===")
        income_result = estimate_income_tax(year)
        saved_files.append(
            str(ROOT / "data" / "processed" / "tax" / f"{year}_종소세예상.json")
        )

    # 3. 세금계산서 누락 탐지
    if args.check_missing or args.all:
        print(f"\n[TAX] === 세금계산서 누락 탐지 ===")
        missing_result = check_missing_invoices(quarter)
        if missing_result.get("missing_count", 0) > 0:
            saved_files.append(
                str(ROOT / "outputs" / "alerts"
                    / f"tax_missing_{TODAY.strftime('%Y%m%d')}.json")
            )

    # 4. 세무 일정 확인
    if args.calendar or args.all:
        print(f"\n[TAX] === 세무 일정 확인 ===")
        calendar_result = check_tax_calendar()

    # 5. 보고서 생성
    report = generate_report(quarter, vat_result, income_result,
                             missing_result, calendar_result)
    report_path = ROOT / "outputs" / "reports" / f"{quarter}_세무준비보고서.txt"
    ensure_dir(report_path.parent)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    saved_files.append(str(report_path))
    print(f"\n[TAX] 보고서 저장: {report_path}")

    print(f"\n[TAX] 완료. 생성 파일 {len(saved_files)}개")
    for fp in saved_files:
        print(f"  - {fp}")


if __name__ == "__main__":
    main()
