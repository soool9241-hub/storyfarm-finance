"""
storyfarm-finance / agents / debt.py
DEBT - 채무 전략가 에이전트

채무 현황 파악, 상환 시뮬레이션(눈사태 전략), 일시상환 D-day 알림,
소액 채무 조기상환 분석을 수행한다.

CLI:
    python agents/debt.py --simulate --extra-payment 500000
    python agents/debt.py --status
    python agents/debt.py --dday
"""

import argparse
import sys
import os
from datetime import datetime, date
from typing import List, Dict, Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir
from tools.calculator import calc_interest

# ──────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────
TODAY = date.today()
YEAR_MONTH = TODAY.strftime("%Y-%m")
TODAY_STR = TODAY.strftime("%Y-%m-%d")
ROOT = get_project_root()
DEBTS_FILE = ROOT / "data" / "debts.json"

OUT_STATUS = ROOT / "data" / "processed" / "debt" / f"{YEAR_MONTH}_채무현황.json"
OUT_SIM = ROOT / "data" / "processed" / "debt" / "상환시뮬레이션.json"
OUT_REPORT = ROOT / "outputs" / "reports" / f"채무관리보고서_{YEAR_MONTH}.txt"
OUT_ALERTS_DIR = ROOT / "outputs" / "alerts"


# ──────────────────────────────────────────────────────────────
# 데이터 로드
# ──────────────────────────────────────────────────────────────
def load_debts() -> List[Dict[str, Any]]:
    """data/debts.json 에서 채무 목록을 로드한다."""
    if not DEBTS_FILE.exists():
        print(f"[DEBT] 경고: 채무 파일 없음 - {DEBTS_FILE}")
        return []
    data = load_json(DEBTS_FILE)
    return data.get("debts", [])


# ──────────────────────────────────────────────────────────────
# 1. 채무 현황 개요
# ──────────────────────────────────────────────────────────────
def build_status(debts: List[Dict]) -> Dict[str, Any]:
    """각 채무의 현재 상태를 정리한다."""
    items = []
    total_balance = 0
    total_monthly = 0

    for d in debts:
        balance = d["current_balance"]
        rate = d["interest_rate"]
        monthly_interest = calc_interest(balance, rate)
        accumulated = _calc_accumulated_interest(d)

        item = {
            "id": d["id"],
            "name": d["name"],
            "type": d["type"],
            "original_amount": d["original_amount"],
            "current_balance": balance,
            "interest_rate": rate,
            "interest_rate_pct": f"{rate * 100:.1f}%",
            "monthly_payment": d["monthly_payment"],
            "monthly_interest": monthly_interest,
            "monthly_principal": max(0, d["monthly_payment"] - monthly_interest),
            "payment_day": d["payment_day"],
            "due_date": d.get("due_date"),
            "start_date": d.get("start_date"),
            "accumulated_interest_estimate": accumulated,
        }

        # 일시상환 D-day
        if d.get("due_date"):
            due = _parse_date(d["due_date"])
            if due:
                delta = (due - TODAY).days
                item["dday"] = delta
                item["dday_label"] = f"D-{delta}" if delta >= 0 else f"D+{abs(delta)}"

        items.append(item)
        total_balance += balance
        total_monthly += d["monthly_payment"]

    total_monthly_interest = sum(i["monthly_interest"] for i in items)

    return {
        "generated_at": TODAY_STR,
        "summary": {
            "total_debt_count": len(items),
            "total_balance": total_balance,
            "total_monthly_payment": total_monthly,
            "total_monthly_interest": total_monthly_interest,
            "total_monthly_principal": total_monthly - total_monthly_interest,
        },
        "debts": items,
    }


def _calc_accumulated_interest(debt: Dict) -> int:
    """시작일부터 오늘까지 대략적인 누적이자를 추정한다."""
    start = _parse_date(debt.get("start_date", ""))
    if not start:
        return 0
    months_elapsed = (TODAY.year - start.year) * 12 + (TODAY.month - start.month)
    if months_elapsed <= 0:
        return 0
    # 간이 추정: 평균 잔액 = (original + current) / 2
    avg_balance = (debt["original_amount"] + debt["current_balance"]) / 2
    return int(avg_balance * debt["interest_rate"] / 12 * months_elapsed)


def _parse_date(s: str) -> date | None:
    """YYYY-MM-DD 문자열을 date로 변환. 실패시 None."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


# ──────────────────────────────────────────────────────────────
# 2. 눈사태(Avalanche) 상환 시뮬레이션
# ──────────────────────────────────────────────────────────────
def simulate_avalanche(debts: List[Dict], extra: int = 0) -> Dict[str, Any]:
    """
    눈사태 전략 시뮬레이션.
    최고금리 채무에 여유 자금을 집중 투입한다.

    Args:
        debts: 채무 리스트
        extra: 월 추가 상환액 (원)

    Returns:
        시뮬레이션 결과 딕셔너리
    """
    # 깊은 복사로 원본 보호
    sim_debts = []
    for d in debts:
        sim_debts.append({
            "id": d["id"],
            "name": d["name"],
            "type": d["type"],
            "balance": d["current_balance"],
            "rate": d["interest_rate"],
            "min_payment": d["monthly_payment"],
            "due_date": d.get("due_date"),
        })

    month = 0
    max_months = 360  # 30년 상한
    total_interest_paid = 0
    total_principal_paid = 0
    monthly_log = []
    payoff_events = []

    while any(sd["balance"] > 0 for sd in sim_debts) and month < max_months:
        month += 1
        month_interest = 0
        month_principal = 0
        remaining_extra = extra

        # 이자 계산 (모든 채무)
        for sd in sim_debts:
            if sd["balance"] <= 0:
                continue
            interest = calc_interest(sd["balance"], sd["rate"])
            sd["_interest"] = interest
            month_interest += interest

        # 최소 납부 적용
        for sd in sim_debts:
            if sd["balance"] <= 0:
                continue
            interest = sd.pop("_interest", 0)
            principal_part = max(0, sd["min_payment"] - interest)

            # 일시상환 채무는 이자만 납부 (만기에 원금 상환)
            if sd["type"] == "bullet_loan":
                principal_part = 0

            actual_principal = min(principal_part, sd["balance"])
            sd["balance"] -= actual_principal
            month_principal += actual_principal
            total_interest_paid += interest

        # 추가 상환분: 금리 높은 순 (일시상환 제외)
        targets = sorted(
            [sd for sd in sim_debts if sd["balance"] > 0 and sd["type"] != "bullet_loan"],
            key=lambda x: x["rate"],
            reverse=True,
        )
        for sd in targets:
            if remaining_extra <= 0:
                break
            apply = min(remaining_extra, sd["balance"])
            sd["balance"] -= apply
            remaining_extra -= apply
            month_principal += apply

        # 일시상환 만기 처리
        for sd in sim_debts:
            if sd["type"] == "bullet_loan" and sd["balance"] > 0 and sd.get("due_date"):
                due = _parse_date(sd["due_date"])
                if due:
                    # 만기 도래 월에 원금 상환 처리
                    due_month = (due.year - TODAY.year) * 12 + (due.month - TODAY.month)
                    if month >= due_month:
                        month_principal += sd["balance"]
                        sd["balance"] = 0

        total_principal_paid += month_principal

        # 상환 완료 이벤트
        for sd in sim_debts:
            if sd["balance"] == 0 and sd["id"] not in [pe["id"] for pe in payoff_events]:
                payoff_events.append({
                    "id": sd["id"],
                    "name": sd["name"],
                    "payoff_month": month,
                    "payoff_date_est": _add_months(TODAY, month).strftime("%Y-%m"),
                })

        monthly_log.append({
            "month": month,
            "interest": month_interest,
            "principal": month_principal,
            "remaining_total": sum(max(0, sd["balance"]) for sd in sim_debts),
        })

    return {
        "extra_payment": extra,
        "total_months": month,
        "payoff_date_est": _add_months(TODAY, month).strftime("%Y-%m"),
        "total_interest_paid": total_interest_paid,
        "total_principal_paid": total_principal_paid,
        "payoff_events": payoff_events,
        "monthly_interest_trend": [
            {"month": ml["month"], "interest": ml["interest"]}
            for ml in monthly_log
        ],
        "monthly_log_summary": monthly_log[:6] + (monthly_log[-3:] if len(monthly_log) > 6 else []),
    }


def _add_months(base: date, months: int) -> date:
    """date에 월수를 더한다."""
    total_months = base.year * 12 + base.month - 1 + months
    year = total_months // 12
    month = total_months % 12 + 1
    day = min(base.day, 28)
    return date(year, month, day)


def run_all_simulations(debts: List[Dict]) -> Dict[str, Any]:
    """시나리오 A/B/C 전체 시뮬레이션을 실행한다."""
    scenario_a = simulate_avalanche(debts, extra=0)
    scenario_b = simulate_avalanche(debts, extra=500_000)
    scenario_c = simulate_avalanche(debts, extra=1_000_000)

    interest_saved_b = scenario_a["total_interest_paid"] - scenario_b["total_interest_paid"]
    interest_saved_c = scenario_a["total_interest_paid"] - scenario_c["total_interest_paid"]

    return {
        "generated_at": TODAY_STR,
        "strategy": "avalanche (highest rate first)",
        "scenarios": {
            "A_minimum_only": {
                "description": "최소 납부만 유지",
                "extra_payment": 0,
                **scenario_a,
            },
            "B_extra_500k": {
                "description": "월 50만원 추가 상환 (최고금리 우선)",
                "extra_payment": 500_000,
                "interest_saved_vs_A": interest_saved_b,
                **scenario_b,
            },
            "C_extra_1M": {
                "description": "월 100만원 추가 상환 (최고금리 우선)",
                "extra_payment": 1_000_000,
                "interest_saved_vs_A": interest_saved_c,
                **scenario_c,
            },
        },
        "recommendation": _pick_recommendation(scenario_a, scenario_b, scenario_c),
    }


def _pick_recommendation(a: Dict, b: Dict, c: Dict) -> str:
    """시뮬레이션 결과로 간단한 추천 문구를 생성한다."""
    saved_b = a["total_interest_paid"] - b["total_interest_paid"]
    saved_c = a["total_interest_paid"] - c["total_interest_paid"]
    lines = []
    lines.append(f"현재 최소납부 유지 시 총 이자: {a['total_interest_paid']:,}원, "
                 f"완납 예상: {a['payoff_date_est']}")
    if saved_b > 0:
        lines.append(f"월 50만원 추가 시 이자 {saved_b:,}원 절감, "
                     f"완납: {b['payoff_date_est']}")
    if saved_c > 0:
        lines.append(f"월 100만원 추가 시 이자 {saved_c:,}원 절감, "
                     f"완납: {c['payoff_date_est']}")
    return " | ".join(lines)


# ──────────────────────────────────────────────────────────────
# 3. 일시상환 D-day 알림
# ──────────────────────────────────────────────────────────────
def check_dday_alerts(debts: List[Dict]) -> List[Dict[str, Any]]:
    """일시상환 채무의 D-day를 확인하고 알림을 생성한다."""
    alerts = []

    for d in debts:
        if d["type"] != "bullet_loan" or not d.get("due_date"):
            continue

        due = _parse_date(d["due_date"])
        if not due:
            continue

        delta = (due - TODAY).days
        level = None
        message = ""

        if delta <= 0:
            level = "overdue"
            message = f"{d['name']} 만기일 경과! (D+{abs(delta)}일) 즉시 조치 필요"
        elif delta <= 7:
            level = "critical"
            message = f"{d['name']} 만기 D-{delta}일! 상환 자금 최종 확인 필요"
        elif delta <= 30:
            level = "urgent"
            message = f"{d['name']} 만기 D-{delta}일. 상환 계획 확정 및 자금 확보 필요"
        elif delta <= 90:
            level = "preparation"
            message = f"{d['name']} 만기 D-{delta}일. 상환 자금 마련 준비 시작 권장"

        if level:
            alerts.append({
                "alert_type": "debt_dday",
                "level": level,
                "debt_id": d["id"],
                "debt_name": d["name"],
                "due_date": d["due_date"],
                "days_remaining": delta,
                "balance": d["current_balance"],
                "message": message,
                "generated_at": TODAY_STR,
            })

    return alerts


def save_dday_alerts(alerts: List[Dict]) -> str | None:
    """D-day 알림을 파일로 저장한다."""
    if not alerts:
        return None
    filepath = OUT_ALERTS_DIR / f"debt_dday_{TODAY.strftime('%Y%m%d')}.json"
    save_json({"alerts": alerts, "generated_at": TODAY_STR}, filepath)
    return str(filepath)


# ──────────────────────────────────────────────────────────────
# 4. 소액 채무 조기상환 분석
# ──────────────────────────────────────────────────────────────
def analyze_small_debts(debts: List[Dict], threshold: int = 1_000_000) -> List[Dict[str, Any]]:
    """잔액이 threshold 미만인 채무의 조기상환 효과를 분석한다."""
    results = []
    for d in debts:
        balance = d["current_balance"]
        if balance <= 0 or balance >= threshold:
            continue
        if d["type"] == "bullet_loan":
            continue

        monthly_interest = calc_interest(balance, d["interest_rate"])
        annual_interest_saved = monthly_interest * 12
        months_left = 0
        if d["monthly_payment"] > 0:
            sim_bal = balance
            while sim_bal > 0 and months_left < 360:
                months_left += 1
                mi = calc_interest(sim_bal, d["interest_rate"])
                principal = d["monthly_payment"] - mi
                if principal <= 0:
                    months_left = 999
                    break
                sim_bal -= principal
            total_interest_if_kept = 0
            sim_bal2 = balance
            for _ in range(months_left):
                mi = calc_interest(sim_bal2, d["interest_rate"])
                total_interest_if_kept += mi
                sim_bal2 -= (d["monthly_payment"] - mi)
                if sim_bal2 <= 0:
                    break

        results.append({
            "id": d["id"],
            "name": d["name"],
            "balance": balance,
            "interest_rate": d["interest_rate"],
            "monthly_interest": monthly_interest,
            "annual_interest_saved": annual_interest_saved,
            "months_remaining": months_left,
            "total_interest_saved_if_payoff": total_interest_if_kept if months_left < 999 else 0,
            "recommendation": (
                f"잔액 {balance:,}원 즉시 상환 시 월 {monthly_interest:,}원, "
                f"연 {annual_interest_saved:,}원 이자 절감"
            ),
        })

    return sorted(results, key=lambda x: x["annual_interest_saved"], reverse=True)


# ──────────────────────────────────────────────────────────────
# 보고서 생성
# ──────────────────────────────────────────────────────────────
def generate_report(status: Dict, sim: Dict | None, alerts: List[Dict],
                    small_debts: List[Dict]) -> str:
    """텍스트 보고서를 생성한다."""
    lines = []
    lines.append("=" * 60)
    lines.append(f"  채무 관리 보고서 - {YEAR_MONTH}")
    lines.append(f"  생성일: {TODAY_STR}")
    lines.append("=" * 60)
    lines.append("")

    # 1. 현황 요약
    s = status["summary"]
    lines.append("[1] 채무 현황 요약")
    lines.append("-" * 40)
    lines.append(f"  총 채무 건수: {s['total_debt_count']}건")
    lines.append(f"  총 잔액:      {s['total_balance']:>15,}원")
    lines.append(f"  월 납부 합계: {s['total_monthly_payment']:>15,}원")
    lines.append(f"  월 이자 합계: {s['total_monthly_interest']:>15,}원")
    lines.append(f"  월 원금 합계: {s['total_monthly_principal']:>15,}원")
    lines.append("")

    # 2. 개별 채무
    lines.append("[2] 개별 채무 상세")
    lines.append("-" * 40)
    for d in status["debts"]:
        lines.append(f"  {d['name']} ({d['id']})")
        lines.append(f"    유형: {d['type']}")
        lines.append(f"    잔액: {d['current_balance']:,}원 / 원금: {d['original_amount']:,}원")
        lines.append(f"    금리: {d['interest_rate_pct']}  |  월 이자: {d['monthly_interest']:,}원")
        lines.append(f"    월 납부: {d['monthly_payment']:,}원  |  납부일: 매월 {d['payment_day']}일")
        if d.get("due_date"):
            lines.append(f"    만기일: {d['due_date']}  |  {d.get('dday_label', '')}")
        lines.append(f"    추정 누적이자: {d['accumulated_interest_estimate']:,}원")
        lines.append("")

    # 3. 시뮬레이션
    if sim:
        lines.append("[3] 상환 시뮬레이션 (눈사태 전략)")
        lines.append("-" * 40)
        for key, sc in sim["scenarios"].items():
            lines.append(f"  시나리오 {key}: {sc.get('description', key)}")
            lines.append(f"    완납 예상: {sc.get('payoff_date_est', 'N/A')}  ({sc.get('total_months', '?')}개월)")
            lines.append(f"    총 이자:   {sc.get('total_interest_paid', 0):>12,}원")
            if "interest_saved_vs_A" in sc:
                lines.append(f"    이자 절감: {sc['interest_saved_vs_A']:>12,}원")
            lines.append("")
        if "recommendation" in sim:
            lines.append(f"  추천: {sim['recommendation']}")
        lines.append("")

    # 4. D-day 알림
    if alerts:
        lines.append("[4] 일시상환 D-day 알림")
        lines.append("-" * 40)
        for a in alerts:
            level_kr = {
                "critical": "[긴급]",
                "urgent": "[주의]",
                "preparation": "[준비]",
                "overdue": "[경과]",
            }.get(a["level"], "[알림]")
            lines.append(f"  {level_kr} {a['message']}")
            lines.append(f"    잔액: {a['balance']:,}원")
        lines.append("")

    # 5. 소액 채무
    if small_debts:
        lines.append("[5] 소액 채무 조기상환 분석 (잔액 100만원 미만)")
        lines.append("-" * 40)
        for sd in small_debts:
            lines.append(f"  {sd['name']}: 잔액 {sd['balance']:,}원")
            lines.append(f"    {sd['recommendation']}")
        lines.append("")

    lines.append("=" * 60)
    lines.append("  DEBT Agent 자동 생성 보고서")
    lines.append("=" * 60)

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="DEBT - 채무 전략가 에이전트",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "예시:\n"
            "  python agents/debt.py --status\n"
            "  python agents/debt.py --simulate --extra-payment 500000\n"
            "  python agents/debt.py --dday\n"
            "  python agents/debt.py --simulate --extra-payment 1000000 --status --dday"
        ),
    )
    parser.add_argument("--status", action="store_true", help="채무 현황 조회")
    parser.add_argument("--simulate", action="store_true", help="상환 시뮬레이션 실행")
    parser.add_argument("--extra-payment", type=int, default=None,
                        help="추가 상환 금액 (원). --simulate와 함께 사용")
    parser.add_argument("--dday", action="store_true", help="일시상환 D-day 알림 확인")
    parser.add_argument("--all", action="store_true", help="모든 분석 실행")

    args = parser.parse_args()

    # 인자 없으면 --all 로 처리
    if not (args.status or args.simulate or args.dday or args.all):
        args.all = True

    debts = load_debts()
    if not debts:
        print("[DEBT] 채무 데이터가 없습니다.")
        return

    print(f"[DEBT] {len(debts)}건의 채무 데이터 로드 완료")

    # 1. 현황
    status = build_status(debts)
    saved_files = []

    if args.status or args.all:
        save_json(status, OUT_STATUS)
        saved_files.append(str(OUT_STATUS))
        print(f"[DEBT] 채무 현황 저장: {OUT_STATUS}")
        s = status["summary"]
        print(f"       총 잔액: {s['total_balance']:,}원 | "
              f"월 이자: {s['total_monthly_interest']:,}원")

    # 2. 시뮬레이션
    sim_result = None
    if args.simulate or args.all:
        if args.extra_payment is not None:
            # 개별 시나리오
            result = simulate_avalanche(debts, extra=args.extra_payment)
            sim_result = {
                "generated_at": TODAY_STR,
                "strategy": "avalanche",
                "scenarios": {
                    "custom": {
                        "description": f"월 {args.extra_payment:,}원 추가 상환",
                        **result,
                    }
                },
            }
            print(f"[DEBT] 시뮬레이션 (추가 {args.extra_payment:,}원): "
                  f"완납 {result['payoff_date_est']}, "
                  f"총이자 {result['total_interest_paid']:,}원")
        else:
            # 전체 시나리오 A/B/C
            sim_result = run_all_simulations(debts)
            for key in ["A_minimum_only", "B_extra_500k", "C_extra_1M"]:
                sc = sim_result["scenarios"][key]
                print(f"[DEBT] 시나리오 {key}: 완납 {sc['payoff_date_est']}, "
                      f"총이자 {sc['total_interest_paid']:,}원")

        save_json(sim_result, OUT_SIM)
        saved_files.append(str(OUT_SIM))
        print(f"[DEBT] 시뮬레이션 결과 저장: {OUT_SIM}")

    # 3. D-day 알림
    alerts = check_dday_alerts(debts)
    if args.dday or args.all:
        if alerts:
            alert_path = save_dday_alerts(alerts)
            if alert_path:
                saved_files.append(alert_path)
            for a in alerts:
                level_icon = {"critical": "!!!", "urgent": "!!", "preparation": "!",
                              "overdue": "XXX"}.get(a["level"], "")
                print(f"[DEBT] {level_icon} {a['message']}")
        else:
            print("[DEBT] 일시상환 D-day 알림 없음")

    # 4. 소액 채무
    small = analyze_small_debts(debts)

    # 5. 보고서 생성
    report = generate_report(status, sim_result, alerts, small)
    ensure_dir(OUT_REPORT.parent)
    with open(OUT_REPORT, "w", encoding="utf-8") as f:
        f.write(report)
    saved_files.append(str(OUT_REPORT))
    print(f"[DEBT] 보고서 저장: {OUT_REPORT}")

    print(f"\n[DEBT] 완료. 생성 파일 {len(saved_files)}개")
    for fp in saved_files:
        print(f"  - {fp}")


if __name__ == "__main__":
    main()
