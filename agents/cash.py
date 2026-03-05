"""
CASH - Cash Flow Forecaster Agent
30일 현금흐름 예측 및 위험 구간 경고

최근 거래 패턴, 확정 수주, 고정비 일정을 바탕으로
30일간의 일별 현금 잔액을 예측하고 위험 구간을 탐지한다.
best / base / worst 시나리오 시뮬레이션을 지원한다.
"""

import argparse
import glob
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir
from tools.calculator import calc_margin

DEFAULT_STARTING_BALANCE = 8_500_000


# ---------------------------------------------------------------------------
# 데이터 로딩
# ---------------------------------------------------------------------------

def load_fixed_expenses() -> Dict[str, Any]:
    """cash_fixed.json 로드. 없으면 기본값 반환."""
    root = get_project_root()
    path = root / "data" / "config" / "cash_fixed.json"
    if path.exists():
        return load_json(path)
    return {
        "monthly_rent": {"amount": 800000, "day": 1},
        "loan_payments": [
            {"name": "국민은행 일시상환", "due_date": "2026-10-31", "amount": 10000000, "monthly_interest": 37500},
            {"name": "카드론", "monthly": 250000, "day": 25},
            {"name": "OK캐피탈", "monthly": 850000, "day": 20},
        ],
        "salary_day": 25,
        "salary_amount": 2000000,
        "card_payment_days": {"국민카드": 15, "신한카드": 17},
        "utilities": {"amount": 300000, "day": 10},
    }


def load_latest_balance() -> int:
    """
    data/processed/ledger/ 에서 가장 최신 잔액을 읽는다.
    파일이 없으면 DEFAULT_STARTING_BALANCE 반환.
    """
    root = get_project_root()
    ledger_dir = root / "data" / "processed" / "ledger"
    if not ledger_dir.exists():
        return DEFAULT_STARTING_BALANCE

    files = sorted(glob.glob(str(ledger_dir / "*.json")), reverse=True)
    for filepath in files:
        try:
            data = load_json(filepath)
            if isinstance(data, dict):
                # 다양한 키 이름 대응
                for key in ["balance", "잔액", "current_balance", "ending_balance"]:
                    if key in data:
                        return int(data[key])
                # 리스트 내 마지막 항목
                if "entries" in data and data["entries"]:
                    last = data["entries"][-1]
                    for key in ["balance", "잔액", "ending_balance"]:
                        if key in last:
                            return int(last[key])
            elif isinstance(data, list) and data:
                last = data[-1]
                for key in ["balance", "잔액", "ending_balance"]:
                    if key in last:
                        return int(last[key])
        except Exception:
            continue

    return DEFAULT_STARTING_BALANCE


def load_pending_orders() -> List[Dict[str, Any]]:
    """
    data/processed/orders/ 에서 미수금(입금 예정) 수주를 로드한다.
    결제 예정일과 금액을 추출한다.
    """
    root = get_project_root()
    order_dir = root / "data" / "processed" / "orders"
    if not order_dir.exists():
        return []

    pending: List[Dict[str, Any]] = []
    for filepath in sorted(glob.glob(str(order_dir / "*.json")), reverse=True):
        try:
            data = load_json(filepath)
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and "orders" in data:
                items = data["orders"]
            elif isinstance(data, dict):
                items = [data]

            for item in items:
                status = str(item.get("payment_status", item.get("status", ""))).lower()
                if status in ("pending", "미수금", "입금예정", "confirmed", "확정"):
                    pending.append({
                        "order_id": item.get("order_id", ""),
                        "amount": int(item.get("revenue", item.get("amount", 0))),
                        "due_date": item.get("payment_due_date", item.get("due_date", "")),
                        "description": item.get("description", ""),
                    })
        except Exception:
            continue

    return pending


def load_recent_transactions(days: int = 90) -> List[Dict[str, Any]]:
    """최근 거래 데이터를 로드하여 수입/지출 패턴을 파악한다."""
    root = get_project_root()
    tx_dir = root / "data" / "processed" / "transactions"
    if not tx_dir.exists():
        return []

    transactions: List[Dict[str, Any]] = []
    for filepath in sorted(glob.glob(str(tx_dir / "*.json")), reverse=True)[:6]:
        try:
            data = load_json(filepath)
            if isinstance(data, list):
                transactions.extend(data)
            elif isinstance(data, dict) and "transactions" in data:
                transactions.extend(data["transactions"])
        except Exception:
            continue

    return transactions


# ---------------------------------------------------------------------------
# 예측 엔진
# ---------------------------------------------------------------------------

def estimate_daily_pattern(transactions: List[Dict]) -> Dict[str, float]:
    """
    과거 거래에서 요일별 평균 수입/지출을 추정한다.
    데이터가 없으면 합리적 기본값을 반환한다.
    """
    # 요일별(0=월~6=일) 수입/지출 집계
    day_income: Dict[int, List[int]] = {i: [] for i in range(7)}
    day_expense: Dict[int, List[int]] = {i: [] for i in range(7)}

    for tx in transactions:
        date_str = tx.get("date", "")
        try:
            dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            weekday = dt.weekday()
        except (ValueError, TypeError):
            continue

        income = int(tx.get("income", tx.get("입금", 0)) or 0)
        expense = int(tx.get("expense", tx.get("출금", 0)) or 0)

        if income > 0:
            day_income[weekday].append(income)
        if expense > 0:
            day_expense[weekday].append(expense)

    # 평균 계산 (데이터가 없는 요일은 0)
    avg_income = {}
    avg_expense = {}
    for d in range(7):
        avg_income[d] = int(sum(day_income[d]) / len(day_income[d])) if day_income[d] else 0
        avg_expense[d] = int(sum(day_expense[d]) / len(day_expense[d])) if day_expense[d] else 0

    return {"avg_income": avg_income, "avg_expense": avg_expense}


def build_fixed_expense_schedule(
    start_date: datetime,
    days: int,
    fixed_config: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    고정비 일정을 날짜별로 매핑한다.
    Returns: { "YYYY-MM-DD": [{"name": ..., "amount": ...}, ...], ... }
    """
    schedule: Dict[str, List[Dict[str, Any]]] = {}

    for day_offset in range(days):
        current = start_date + timedelta(days=day_offset)
        date_key = current.strftime("%Y-%m-%d")
        day_of_month = current.day
        entries: List[Dict[str, Any]] = []

        # 월세
        rent = fixed_config.get("monthly_rent", {})
        if rent and day_of_month == rent.get("day", 1):
            entries.append({"name": "월세", "amount": rent["amount"]})

        # 대출 상환
        for loan in fixed_config.get("loan_payments", []):
            loan_day = loan.get("day")
            monthly_amount = loan.get("monthly", 0)
            monthly_interest = loan.get("monthly_interest", 0)

            if loan_day and day_of_month == loan_day:
                amount = monthly_amount if monthly_amount else monthly_interest
                if amount > 0:
                    entries.append({"name": loan["name"], "amount": amount})

            # 일시상환 대출: 월 이자만 납부 (15일 기본)
            if not loan_day and monthly_interest and day_of_month == 15:
                entries.append({
                    "name": f"{loan['name']} 이자",
                    "amount": monthly_interest,
                })

        # 급여
        salary_day = fixed_config.get("salary_day", 25)
        salary_amount = fixed_config.get("salary_amount", 2000000)
        if day_of_month == salary_day:
            entries.append({"name": "급여", "amount": salary_amount})

        # 카드 결제
        card_days = fixed_config.get("card_payment_days", {})
        for card_name, card_day in card_days.items():
            if day_of_month == card_day:
                # 카드 결제 금액은 거래 패턴에서 추정 (기본 500,000)
                entries.append({"name": f"{card_name} 결제", "amount": 500000})

        # 공과금
        utilities = fixed_config.get("utilities", {})
        if utilities and day_of_month == utilities.get("day", 10):
            entries.append({"name": "공과금", "amount": utilities["amount"]})

        if entries:
            schedule[date_key] = entries

    return schedule


def build_income_schedule(
    start_date: datetime,
    days: int,
    pending_orders: List[Dict[str, Any]],
    daily_pattern: Dict[str, Any],
    scenario: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    수입 예측 일정을 구성한다.
    - 확정 수주의 입금 예정일
    - 요일별 평균 수입 패턴
    """
    scenario_rate = {"best": 1.0, "base": 0.8, "worst": 0.6}.get(scenario, 0.8)
    schedule: Dict[str, List[Dict[str, Any]]] = {}

    # 확정 수주 입금
    for order in pending_orders:
        due = order.get("due_date", "")
        if not due:
            continue
        try:
            due_dt = datetime.strptime(due[:10], "%Y-%m-%d")
            if start_date <= due_dt < start_date + timedelta(days=days):
                date_key = due_dt.strftime("%Y-%m-%d")
                amount = int(order["amount"] * scenario_rate)
                if date_key not in schedule:
                    schedule[date_key] = []
                schedule[date_key].append({
                    "name": f"수주입금: {order.get('order_id', '')}",
                    "amount": amount,
                })
        except (ValueError, TypeError):
            continue

    # 요일별 패턴 수입 (확정 수주가 없는 날)
    avg_income = daily_pattern.get("avg_income", {})
    for day_offset in range(days):
        current = start_date + timedelta(days=day_offset)
        date_key = current.strftime("%Y-%m-%d")
        weekday = current.weekday()

        # 주말은 수입 없음 (공방 기준)
        if weekday >= 5:
            continue

        pattern_income = avg_income.get(weekday, 0)
        if pattern_income > 0 and date_key not in schedule:
            adjusted = int(pattern_income * scenario_rate)
            if adjusted > 0:
                if date_key not in schedule:
                    schedule[date_key] = []
                schedule[date_key].append({
                    "name": "패턴 기반 예상 수입",
                    "amount": adjusted,
                })

    return schedule


def forecast_cashflow(
    start_date: datetime,
    days: int,
    scenario: str = "base",
) -> Dict[str, Any]:
    """
    30일간 일별 현금흐름을 예측한다.

    Returns:
        {
            "start_date": ...,
            "days": ...,
            "scenario": ...,
            "starting_balance": ...,
            "daily_forecast": [ { date, income, expense, balance, status, details }, ... ],
            "danger_zones": [ ... ],
            "summary": { ... },
        }
    """
    # 데이터 로드
    starting_balance = load_latest_balance()
    fixed_config = load_fixed_expenses()
    pending_orders = load_pending_orders()
    recent_tx = load_recent_transactions()
    daily_pattern = estimate_daily_pattern(recent_tx)

    # 일정 생성
    expense_schedule = build_fixed_expense_schedule(start_date, days, fixed_config)
    income_schedule = build_income_schedule(
        start_date, days, pending_orders, daily_pattern, scenario,
    )

    # 일별 예측
    balance = starting_balance
    daily_forecast: List[Dict[str, Any]] = []
    danger_zones: List[Dict[str, Any]] = []

    for day_offset in range(days):
        current = start_date + timedelta(days=day_offset)
        date_key = current.strftime("%Y-%m-%d")

        # 수입 합계
        income_items = income_schedule.get(date_key, [])
        day_income = sum(item["amount"] for item in income_items)

        # 지출 합계
        expense_items = expense_schedule.get(date_key, [])
        day_expense = sum(item["amount"] for item in expense_items)

        # 잔액 갱신
        balance = balance + day_income - day_expense

        # 상태 판정
        if balance < 0:
            status = "CRITICAL"
        elif balance < 2_000_000:
            status = "DANGER"
        elif balance < 5_000_000:
            status = "WARNING"
        else:
            status = "OK"

        day_record = {
            "date": date_key,
            "weekday": ["월", "화", "수", "목", "금", "토", "일"][current.weekday()],
            "income": day_income,
            "expense": day_expense,
            "balance": balance,
            "status": status,
            "income_details": income_items,
            "expense_details": expense_items,
        }
        daily_forecast.append(day_record)

        # 위험 구간 기록
        if status in ("CRITICAL", "DANGER", "WARNING"):
            danger_zones.append({
                "date": date_key,
                "status": status,
                "balance": balance,
                "days_from_start": day_offset,
            })

    # 요약
    min_balance = min(d["balance"] for d in daily_forecast)
    min_date = next(d["date"] for d in daily_forecast if d["balance"] == min_balance)
    max_balance = max(d["balance"] for d in daily_forecast)
    total_income = sum(d["income"] for d in daily_forecast)
    total_expense = sum(d["expense"] for d in daily_forecast)

    summary = {
        "starting_balance": starting_balance,
        "ending_balance": daily_forecast[-1]["balance"] if daily_forecast else starting_balance,
        "min_balance": min_balance,
        "min_balance_date": min_date,
        "max_balance": max_balance,
        "total_income": total_income,
        "total_expense": total_expense,
        "net_change": total_income - total_expense,
        "danger_days": len([d for d in danger_zones if d["status"] in ("DANGER", "CRITICAL")]),
        "warning_days": len([d for d in danger_zones if d["status"] == "WARNING"]),
        "critical_days": len([d for d in danger_zones if d["status"] == "CRITICAL"]),
    }

    return {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "days": days,
        "scenario": scenario,
        "starting_balance": starting_balance,
        "daily_forecast": daily_forecast,
        "danger_zones": danger_zones,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# 시나리오 비교
# ---------------------------------------------------------------------------

def run_all_scenarios(
    start_date: datetime,
    days: int,
) -> Dict[str, Dict[str, Any]]:
    """best / base / worst 세 시나리오를 모두 실행하여 비교."""
    results = {}
    for scenario in ("best", "base", "worst"):
        results[scenario] = forecast_cashflow(start_date, days, scenario)
    return results


# ---------------------------------------------------------------------------
# 출력 포매팅
# ---------------------------------------------------------------------------

def format_calendar(forecast: Dict[str, Any]) -> str:
    """현금흐름 캘린더를 텍스트 테이블로 포맷한다."""
    lines: List[str] = []
    scenario = forecast["scenario"]
    scenario_label = {"best": "낙관", "base": "기본", "worst": "비관"}.get(scenario, scenario)

    lines.append("=" * 78)
    lines.append(f"  현금흐름 캘린더 ({scenario_label} 시나리오)")
    lines.append(f"  기간: {forecast['start_date']} ~ {forecast['days']}일간")
    lines.append(f"  시작 잔액: {forecast['starting_balance']:>12,}원")
    lines.append("=" * 78)
    lines.append("")

    header = f"{'날짜':^12}{'요일':^4}{'수입':>14}{'지출':>14}{'잔액':>14}{'상태':^10}"
    lines.append(header)
    lines.append("-" * 78)

    for day in forecast["daily_forecast"]:
        status_mark = {
            "OK": "    ",
            "WARNING": " [!]",
            "DANGER": "[!!]",
            "CRITICAL": "[XX]",
        }.get(day["status"], "    ")

        line = (
            f"{day['date']:^12}"
            f"{day['weekday']:^4}"
            f"{day['income']:>14,}"
            f"{day['expense']:>14,}"
            f"{day['balance']:>14,}"
            f"{status_mark:^10}"
        )
        lines.append(line)

        # 지출 상세 (고정비 항목이 있는 날)
        if day["expense_details"]:
            for item in day["expense_details"]:
                lines.append(f"{'':>16}  └ {item['name']}: {item['amount']:>10,}원")

    lines.append("-" * 78)

    # 요약
    summary = forecast["summary"]
    lines.append("")
    lines.append("  [요약]")
    lines.append(f"  총 수입         : {summary['total_income']:>14,}원")
    lines.append(f"  총 지출         : {summary['total_expense']:>14,}원")
    lines.append(f"  순 변동         : {summary['net_change']:>14,}원")
    lines.append(f"  최저 잔액       : {summary['min_balance']:>14,}원 ({summary['min_balance_date']})")
    lines.append(f"  최종 잔액       : {summary['ending_balance']:>14,}원")
    lines.append(f"  경고 일수       : {summary['warning_days']}일")
    lines.append(f"  위험 일수       : {summary['danger_days']}일")
    lines.append(f"  심각 일수       : {summary['critical_days']}일")
    lines.append("=" * 78)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------

def run_cash(
    start_date_str: str,
    days: int = 30,
    scenario: str = "base",
) -> None:
    """CASH 에이전트 메인 실행 함수."""
    root = get_project_root()
    today_str = datetime.now().strftime("%Y%m%d")

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"[CASH] 날짜 형식 오류: {start_date_str} (YYYY-MM-DD 형식 필요)")
        return

    month_str = start_date.strftime("%Y-%m")
    print(f"[CASH] 현금흐름 예측 시작")
    print(f"[CASH] 시작일: {start_date_str}, 기간: {days}일, 시나리오: {scenario}")

    # ── 1. 예측 실행 ──
    forecast = forecast_cashflow(start_date, days, scenario)
    summary = forecast["summary"]

    print(f"[CASH] 시작 잔액: {forecast['starting_balance']:,}원")
    print(f"[CASH] 예측 완료 - {days}일간 분석")

    # ── 2. 시나리오 비교 ──
    print(f"\n[CASH] 3개 시나리오 비교 실행 중...")
    all_scenarios = run_all_scenarios(start_date, days)

    print(f"\n{'시나리오':^10}{'최종잔액':>14}{'최저잔액':>14}{'위험일수':>8}")
    print("-" * 50)
    for sc_name, sc_data in all_scenarios.items():
        sc_label = {"best": "낙관", "base": "기본", "worst": "비관"}.get(sc_name, sc_name)
        sc_sum = sc_data["summary"]
        print(
            f"{sc_label:^10}"
            f"{sc_sum['ending_balance']:>14,}"
            f"{sc_sum['min_balance']:>14,}"
            f"{sc_sum['danger_days'] + sc_sum['critical_days']:>8}"
        )

    # ── 3. 위험 구간 경고 ──
    danger_zones = forecast["danger_zones"]
    critical_within_7 = [
        d for d in danger_zones
        if d["days_from_start"] <= 7 and d["status"] in ("DANGER", "CRITICAL")
    ]

    if danger_zones:
        print(f"\n[CASH] 위험 구간 탐지: {len(danger_zones)}일")
        for dz in danger_zones[:10]:  # 최대 10개 출력
            icon = {"WARNING": "!", "DANGER": "!!", "CRITICAL": "XXX"}.get(dz["status"], "?")
            print(f"  [{icon}] {dz['date']}: 잔액 {dz['balance']:,}원 ({dz['status']})")

        if len(danger_zones) > 10:
            print(f"  ... 외 {len(danger_zones) - 10}일")

    if critical_within_7:
        print(f"\n[CASH] 7일 이내 잔액 200만원 미만 위험: {len(critical_within_7)}일!")

    # ── 4. 결과 저장 ──

    # 30일 예측 JSON
    cashflow_dir = root / "data" / "processed" / "cashflow"
    forecast_path = cashflow_dir / f"{month_str}_30일예측.json"
    save_json(
        {
            "generated_at": datetime.now().isoformat(),
            "scenario": scenario,
            "start_date": start_date_str,
            "days": days,
            "starting_balance": forecast["starting_balance"],
            "summary": summary,
            "daily_forecast": forecast["daily_forecast"],
            "scenarios_comparison": {
                sc_name: sc_data["summary"]
                for sc_name, sc_data in all_scenarios.items()
            },
        },
        forecast_path,
    )
    print(f"\n[CASH] 예측 저장: {forecast_path}")

    # 캘린더 텍스트
    report_dir = root / "outputs" / "reports"
    ensure_dir(report_dir)
    calendar_path = report_dir / f"{month_str}_현금흐름캘린더.txt"
    calendar_text = format_calendar(forecast)
    with open(calendar_path, "w", encoding="utf-8") as f:
        f.write(calendar_text)
    print(f"[CASH] 캘린더 저장: {calendar_path}")

    # 위험 경고 JSON (위험 구간 존재 시)
    if danger_zones:
        alert_path = root / "outputs" / "alerts" / f"cash_danger_{today_str}.json"
        save_json(
            {
                "generated_at": datetime.now().isoformat(),
                "scenario": scenario,
                "start_date": start_date_str,
                "days": days,
                "starting_balance": forecast["starting_balance"],
                "danger_zone_count": len(danger_zones),
                "critical_within_7_days": len(critical_within_7),
                "danger_zones": danger_zones,
                "summary": summary,
            },
            alert_path,
        )
        print(f"[CASH] 위험 경고 저장: {alert_path}")

    # ── 5. 최종 요약 ──
    print(f"\n{'=' * 60}")
    print(f"  CASH 현금흐름 예측 요약 - {scenario} 시나리오")
    print(f"{'=' * 60}")
    print(f"  예측 기간        : {start_date_str} ~ {days}일")
    print(f"  시작 잔액        : {forecast['starting_balance']:>14,}원")
    print(f"  최종 예상 잔액   : {summary['ending_balance']:>14,}원")
    print(f"  최저 예상 잔액   : {summary['min_balance']:>14,}원 ({summary['min_balance_date']})")
    print(f"  총 예상 수입     : {summary['total_income']:>14,}원")
    print(f"  총 예상 지출     : {summary['total_expense']:>14,}원")
    print(f"  경고/위험/심각   : {summary['warning_days']}일 / {summary['danger_days']}일 / {summary['critical_days']}일")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CASH - 30일 현금흐름 예측 에이전트",
    )
    parser.add_argument(
        "--from",
        dest="from_date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="예측 시작일 (YYYY-MM-DD, 기본: 오늘)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="예측 기간 일수 (기본: 30)",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        choices=["best", "base", "worst"],
        default="base",
        help="시나리오 (best/base/worst, 기본: base)",
    )
    args = parser.parse_args()

    run_cash(
        start_date_str=args.from_date,
        days=args.days,
        scenario=args.scenario,
    )


if __name__ == "__main__":
    main()
