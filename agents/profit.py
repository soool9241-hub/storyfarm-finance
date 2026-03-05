"""
storyfarm-finance / agents / profit.py
PROFIT - 수익성 분석 에이전트

공방(workshop)과 펜션(pension) 사업의 수익성을 분석한다.

읽는 데이터:
  - data/processed/transactions/  (FELIX 출력)
  - data/processed/ledger/        (LUNA 출력)
  - data/processed/cost/          (MARCO 출력)

출력:
  - data/processed/profit/YYYY-MM_수익성분석.json
  - outputs/reports/YYYY-MM_수익성리포트.txt
"""

import sys
import os
import argparse
import glob
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir
from tools.calculator import calc_depreciation_straight


# ---------------------------------------------------------------------------
# 데이터 로드 헬퍼
# ---------------------------------------------------------------------------

def _load_all_json_in(directory: str) -> list[dict]:
    """디렉터리 내 모든 JSON 파일을 읽어 리스트로 합친다."""
    results = []
    if not os.path.isdir(directory):
        return results
    for fp in sorted(glob.glob(os.path.join(directory, "*.json"))):
        try:
            data = load_json(fp)
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
        except Exception as e:
            print(f"[PROFIT] 파일 로드 실패 ({fp}): {e}")
    return results


def _filter_month(records: list[dict], month: str, date_key: str = "date") -> list[dict]:
    """YYYY-MM 형식으로 해당 월 레코드만 필터링한다."""
    return [r for r in records if r.get(date_key, "").startswith(month)]


def _safe_div(a, b, default=0.0):
    return a / b if b != 0 else default


# ---------------------------------------------------------------------------
# 1. 사업 비교 (공방 vs 펜션)
# ---------------------------------------------------------------------------

def analyze_business_comparison(transactions: list[dict], costs: list[dict],
                                month: str) -> dict:
    """공방/펜션 사업별 매출, 매출원가, 영업이익, 마진율을 계산한다."""
    biz_types = ["workshop", "pension"]
    result = {}

    for biz in biz_types:
        biz_txns = [t for t in transactions
                    if t.get("business_type") == biz]
        biz_costs = [c for c in costs
                     if c.get("business_type") == biz]

        revenue = sum(t.get("amount", 0) for t in biz_txns
                      if t.get("type") in ("income", "매출", "입금"))
        cogs = sum(c.get("total_cost", c.get("cost", 0)) for c in biz_costs)
        operating_profit = revenue - cogs
        margin = _safe_div(operating_profit, revenue) * 100

        result[biz] = {
            "revenue": int(revenue),
            "cogs": int(cogs),
            "operating_profit": int(operating_profit),
            "profit_margin_pct": round(margin, 1),
        }

    # 한글 요약
    w_margin = result.get("workshop", {}).get("profit_margin_pct", 0)
    p_margin = result.get("pension", {}).get("profit_margin_pct", 0)
    result["summary"] = f"공방 마진 {w_margin}%, 펜션 마진 {p_margin}%"

    return result


# ---------------------------------------------------------------------------
# 2. 고객·소재별 마진 랭킹 (공방)
# ---------------------------------------------------------------------------

def analyze_material_margin(costs: list[dict]) -> list[dict]:
    """소재별(AL6061 / SUS304 / MDF 등) 마진 랭킹을 계산한다."""
    by_material: dict[str, dict] = defaultdict(lambda: {"revenue": 0, "cost": 0})

    for c in costs:
        if c.get("business_type") != "workshop":
            continue
        mat = c.get("material", c.get("소재", "기타"))
        by_material[mat]["revenue"] += c.get("revenue", c.get("selling_price", 0))
        by_material[mat]["cost"] += c.get("total_cost", c.get("cost", 0))

    rankings = []
    for mat, v in by_material.items():
        profit = v["revenue"] - v["cost"]
        margin = _safe_div(profit, v["revenue"]) * 100
        rankings.append({
            "material": mat,
            "revenue": int(v["revenue"]),
            "cost": int(v["cost"]),
            "profit": int(profit),
            "margin_pct": round(margin, 1),
        })

    rankings.sort(key=lambda x: x["margin_pct"], reverse=True)
    return rankings


def analyze_customer_margin(transactions: list[dict], costs: list[dict]) -> dict:
    """고객별 매출·마진 순위 및 고매출 저마진 고객 플래그."""
    by_customer: dict[str, dict] = defaultdict(lambda: {"revenue": 0, "cost": 0})

    for t in transactions:
        if t.get("business_type") != "workshop":
            continue
        cust = t.get("customer", t.get("거래처", "미분류"))
        if t.get("type") in ("income", "매출", "입금"):
            by_customer[cust]["revenue"] += t.get("amount", 0)

    for c in costs:
        if c.get("business_type") != "workshop":
            continue
        cust = c.get("customer", c.get("거래처", "미분류"))
        by_customer[cust]["cost"] += c.get("total_cost", c.get("cost", 0))

    rankings = []
    for cust, v in by_customer.items():
        profit = v["revenue"] - v["cost"]
        margin = _safe_div(profit, v["revenue"]) * 100
        rankings.append({
            "customer": cust,
            "revenue": int(v["revenue"]),
            "cost": int(v["cost"]),
            "profit": int(profit),
            "margin_pct": round(margin, 1),
        })

    # 매출 상위 10
    by_revenue = sorted(rankings, key=lambda x: x["revenue"], reverse=True)[:10]
    # 마진 상위 10
    by_margin = sorted(rankings, key=lambda x: x["margin_pct"], reverse=True)[:10]

    # 고매출 저마진 플래그: 매출 상위 10인데 마진 15% 미만
    flagged = [c for c in by_revenue if c["margin_pct"] < 15.0]

    return {
        "top10_by_revenue": by_revenue,
        "top10_by_margin": by_margin,
        "high_revenue_low_margin": flagged,
    }


# ---------------------------------------------------------------------------
# 3. 펜션 분석
# ---------------------------------------------------------------------------

_WEEKDAY_NAMES_KR = ["월", "화", "수", "목", "금", "토", "일"]
_SEASON_MAP = {
    1: "겨울", 2: "겨울", 3: "봄", 4: "봄", 5: "봄",
    6: "여름", 7: "여름", 8: "여름", 9: "가을", 10: "가을",
    11: "가을", 12: "겨울",
}


def analyze_pension(transactions: list[dict], month: str) -> dict:
    """펜션 매출: 요일별·계절별 매출, 1인당 매출, 공실률."""
    pension_txns = [t for t in transactions
                    if t.get("business_type") == "pension"
                    and t.get("type") in ("income", "매출", "입금")]

    # 요일별 매출
    by_weekday: dict[str, int] = defaultdict(int)
    # 계절별 매출
    by_season: dict[str, int] = defaultdict(int)
    total_revenue = 0
    total_guests = 0
    booked_days: set[str] = set()

    for t in pension_txns:
        date_str = t.get("date", "")
        amount = t.get("amount", 0)
        guests = t.get("guests", t.get("인원", 1))
        total_revenue += amount
        total_guests += guests

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            wd = _WEEKDAY_NAMES_KR[dt.weekday()]
            by_weekday[wd] += amount
            season = _SEASON_MAP.get(dt.month, "기타")
            by_season[season] += amount
            booked_days.add(date_str)
        except (ValueError, IndexError):
            pass

    # 해당 월의 총 일수
    try:
        year, mon = map(int, month.split("-"))
        if mon == 12:
            next_month_start = datetime(year + 1, 1, 1)
        else:
            next_month_start = datetime(year, mon + 1, 1)
        month_start = datetime(year, mon, 1)
        total_days = (next_month_start - month_start).days
    except Exception:
        total_days = 30

    unbooked = total_days - len(booked_days)
    vacancy_rate = _safe_div(unbooked, total_days) * 100
    avg_per_guest = _safe_div(total_revenue, total_guests)

    return {
        "revenue_by_weekday": dict(by_weekday),
        "revenue_by_season": dict(by_season),
        "total_revenue": int(total_revenue),
        "total_guests": int(total_guests),
        "avg_revenue_per_guest": int(avg_per_guest),
        "total_days": total_days,
        "booked_days": len(booked_days),
        "unbooked_days": unbooked,
        "vacancy_rate_pct": round(vacancy_rate, 1),
    }


# ---------------------------------------------------------------------------
# 4. 성장 분석 (MoM)
# ---------------------------------------------------------------------------

def _prev_month(month: str) -> str:
    """YYYY-MM 문자열의 전월을 반환한다."""
    year, mon = map(int, month.split("-"))
    if mon == 1:
        return f"{year - 1}-12"
    return f"{year}-{mon - 1:02d}"


def analyze_growth(transactions: list[dict], month: str) -> dict:
    """전월 대비 성장률(MoM)을 계산하고, -20% 이하이면 경고."""
    prev = _prev_month(month)

    current_rev = sum(
        t.get("amount", 0)
        for t in transactions
        if t.get("date", "").startswith(month)
        and t.get("type") in ("income", "매출", "입금")
    )
    prev_rev = sum(
        t.get("amount", 0)
        for t in transactions
        if t.get("date", "").startswith(prev)
        and t.get("type") in ("income", "매출", "입금")
    )

    if prev_rev == 0:
        mom_pct = None
        alert = False
    else:
        mom_pct = round((current_rev - prev_rev) / prev_rev * 100, 1)
        alert = mom_pct < -20

    result = {
        "current_month": month,
        "current_revenue": int(current_rev),
        "previous_month": prev,
        "previous_revenue": int(prev_rev),
        "mom_pct": mom_pct,
        "alert": alert,
    }
    if alert:
        result["alert_message"] = (
            f"[경고] {month} 매출이 전월 대비 {mom_pct}% 감소했습니다."
        )
    return result


# ---------------------------------------------------------------------------
# 5. 손익분기점(BEP) 분석
# ---------------------------------------------------------------------------

def analyze_breakeven(transactions: list[dict], costs: list[dict],
                      month: str) -> dict:
    """고정비·변동비를 구분하여 BEP를 계산한다."""
    root = get_project_root()

    # 고정비: 임대료, 급여, 감가상각, 대출이자
    fixed_costs = 0
    cash_fixed_path = root / "data" / "config" / "cash_fixed.json"
    if cash_fixed_path.exists():
        cfg = load_json(cash_fixed_path)
        fixed_costs += cfg.get("monthly_rent", {}).get("amount", 0)
        fixed_costs += cfg.get("salary_amount", 0)
        fixed_costs += cfg.get("utilities", {}).get("amount", 0)
        for loan in cfg.get("loan_payments", []):
            fixed_costs += loan.get("monthly_interest", loan.get("monthly", 0))

    # 감가상각 합산
    assets_path = root / "data" / "assets.json"
    if assets_path.exists():
        assets_data = load_json(assets_path)
        for asset in assets_data.get("assets", []):
            # calc_depreciation_straight(cost, residual_rate, years, months_elapsed)
            dep = calc_depreciation_straight(
                asset["acquisition_cost"],
                asset.get("residual_value_rate", 0.10),
                asset["useful_life_years"],
                1,  # 1개월 경과 기준으로 월 상각비 산출
            )
            fixed_costs += dep["monthly_amount"]

    # 변동비: 재료비, 외주비
    month_costs = [c for c in costs if c.get("date", "").startswith(month)]
    variable_costs = sum(
        c.get("material_cost", 0) + c.get("outsourcing_cost", 0)
        for c in month_costs
    )
    # 변동비가 없으면 total_cost의 합을 대체 사용
    if variable_costs == 0:
        variable_costs = sum(c.get("total_cost", c.get("cost", 0))
                             for c in month_costs)

    # 매출
    month_txns = [t for t in transactions
                  if t.get("date", "").startswith(month)
                  and t.get("type") in ("income", "매출", "입금")]
    revenue = sum(t.get("amount", 0) for t in month_txns)

    variable_cost_ratio = _safe_div(variable_costs, revenue)
    contribution_margin_ratio = 1 - variable_cost_ratio

    if contribution_margin_ratio > 0:
        bep = int(fixed_costs / contribution_margin_ratio)
    else:
        bep = 0

    gap = bep - int(revenue)

    if gap > 0:
        bep_message = f"이번달 BEP 달성까지 {gap // 10000}만원 남았습니다"
    else:
        bep_message = "BEP 초과 달성!"

    return {
        "fixed_costs": int(fixed_costs),
        "variable_costs": int(variable_costs),
        "variable_cost_ratio": round(variable_cost_ratio, 4),
        "revenue": int(revenue),
        "bep": bep,
        "gap": gap,
        "bep_message": bep_message,
    }


# ---------------------------------------------------------------------------
# 리포트 텍스트 생성
# ---------------------------------------------------------------------------

def generate_report_text(month: str, result: dict) -> str:
    """분석 결과를 사람이 읽을 수 있는 텍스트 리포트로 변환한다."""
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"  PROFIT - {month} 수익성 분석 리포트")
    lines.append(f"  생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"{'='*60}")
    lines.append("")

    # 사업 비교
    biz = result.get("business_comparison", {})
    lines.append("[1] 사업 비교 (공방 vs 펜션)")
    lines.append("-" * 40)
    for btype, label in [("workshop", "공방"), ("pension", "펜션")]:
        info = biz.get(btype, {})
        lines.append(f"  {label}:")
        lines.append(f"    매출:       {info.get('revenue', 0):>12,}원")
        lines.append(f"    매출원가:   {info.get('cogs', 0):>12,}원")
        lines.append(f"    영업이익:   {info.get('operating_profit', 0):>12,}원")
        lines.append(f"    마진율:     {info.get('profit_margin_pct', 0):>10.1f}%")
    lines.append(f"  >> {biz.get('summary', '')}")
    lines.append("")

    # 소재별 마진
    mat = result.get("material_margin", [])
    lines.append("[2] 소재별 마진 랭킹 (공방)")
    lines.append("-" * 40)
    if mat:
        for i, m in enumerate(mat, 1):
            lines.append(
                f"  {i}. {m['material']}: 매출 {m['revenue']:,}원, "
                f"마진 {m['margin_pct']}%"
            )
    else:
        lines.append("  (데이터 없음)")
    lines.append("")

    # 고객별 마진
    cust = result.get("customer_margin", {})
    flagged = cust.get("high_revenue_low_margin", [])
    lines.append("[3] 고객별 매출 TOP 10 (공방)")
    lines.append("-" * 40)
    for i, c in enumerate(cust.get("top10_by_revenue", []), 1):
        lines.append(
            f"  {i}. {c['customer']}: 매출 {c['revenue']:,}원, "
            f"마진 {c['margin_pct']}%"
        )
    if flagged:
        lines.append("")
        lines.append("  [주의] 고매출 저마진 고객:")
        for c in flagged:
            lines.append(
                f"    - {c['customer']}: 매출 {c['revenue']:,}원, "
                f"마진 {c['margin_pct']}%"
            )
    lines.append("")

    # 펜션 분석
    pen = result.get("pension_analysis", {})
    lines.append("[4] 펜션 분석")
    lines.append("-" * 40)
    lines.append(f"  총 매출:         {pen.get('total_revenue', 0):>12,}원")
    lines.append(f"  총 투숙객:       {pen.get('total_guests', 0):>10}명")
    lines.append(f"  1인당 평균매출:  {pen.get('avg_revenue_per_guest', 0):>12,}원")
    lines.append(f"  공실률:          {pen.get('vacancy_rate_pct', 0):>10.1f}%")
    wd = pen.get("revenue_by_weekday", {})
    if wd:
        lines.append("  요일별 매출:")
        for day in ["월", "화", "수", "목", "금", "토", "일"]:
            if day in wd:
                lines.append(f"    {day}: {wd[day]:>12,}원")
    lines.append("")

    # 성장 분석
    growth = result.get("growth", {})
    lines.append("[5] 성장 분석 (MoM)")
    lines.append("-" * 40)
    lines.append(f"  당월 매출:   {growth.get('current_revenue', 0):>12,}원")
    lines.append(f"  전월 매출:   {growth.get('previous_revenue', 0):>12,}원")
    mom = growth.get("mom_pct")
    if mom is not None:
        lines.append(f"  전월대비:    {mom:>+10.1f}%")
    else:
        lines.append("  전월대비:    (전월 데이터 없음)")
    if growth.get("alert"):
        lines.append(f"  ** {growth['alert_message']} **")
    lines.append("")

    # BEP
    bep = result.get("breakeven", {})
    lines.append("[6] 손익분기점(BEP) 분석")
    lines.append("-" * 40)
    lines.append(f"  고정비:      {bep.get('fixed_costs', 0):>12,}원")
    lines.append(f"  변동비:      {bep.get('variable_costs', 0):>12,}원")
    lines.append(f"  변동비율:    {bep.get('variable_cost_ratio', 0):>10.2%}")
    lines.append(f"  당월매출:    {bep.get('revenue', 0):>12,}원")
    lines.append(f"  BEP:         {bep.get('bep', 0):>12,}원")
    lines.append(f"  >> {bep.get('bep_message', '')}")
    lines.append("")
    lines.append(f"{'='*60}")
    lines.append("  End of Report")
    lines.append(f"{'='*60}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------

def run(month: str, compare: list[str] | None = None):
    """PROFIT 에이전트 메인 로직."""
    root = get_project_root()
    print(f"[PROFIT] {month} 수익성 분석 시작")

    # 데이터 로드
    txn_dir = str(root / "data" / "processed" / "transactions")
    ledger_dir = str(root / "data" / "processed" / "ledger")
    cost_dir = str(root / "data" / "processed" / "cost")

    all_transactions = _load_all_json_in(txn_dir) + _load_all_json_in(ledger_dir)
    all_costs = _load_all_json_in(cost_dir)

    # 해당 월 + 전월 필터 (성장 분석을 위해 전월도 포함)
    prev = _prev_month(month)
    transactions = [
        t for t in all_transactions
        if t.get("date", "").startswith(month)
        or t.get("date", "").startswith(prev)
    ]
    costs = _filter_month(all_costs, month)

    print(f"[PROFIT] 거래 {len(transactions)}건, 원가 {len(costs)}건 로드됨")

    # 분석 실행
    result = {
        "month": month,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    result["business_comparison"] = analyze_business_comparison(
        transactions, costs, month
    )
    result["material_margin"] = analyze_material_margin(costs)
    result["customer_margin"] = analyze_customer_margin(transactions, costs)
    result["pension_analysis"] = analyze_pension(transactions, month)
    result["growth"] = analyze_growth(transactions, month)
    result["breakeven"] = analyze_breakeven(all_transactions, all_costs, month)

    # compare 모드: 특정 사업만 비교 출력
    if compare:
        print(f"\n[비교 모드] {' vs '.join(compare)}")
        for biz in compare:
            info = result["business_comparison"].get(biz, {})
            label = "공방" if biz == "workshop" else "펜션"
            print(f"  {label}: 매출 {info.get('revenue', 0):,}원, "
                  f"마진 {info.get('profit_margin_pct', 0)}%")

    # 성장 경고 → alerts 저장
    growth = result["growth"]
    if growth.get("alert"):
        alert_path = (root / "outputs" / "alerts"
                      / f"profit_growth_alert_{month}.json")
        save_json({
            "type": "growth_decline",
            "month": month,
            "mom_pct": growth["mom_pct"],
            "message": growth["alert_message"],
            "generated_at": result["generated_at"],
        }, alert_path)
        print(f"[PROFIT] 성장 경고 저장: {alert_path}")

    # JSON 저장
    json_path = root / "data" / "processed" / "profit" / f"{month}_수익성분석.json"
    save_json(result, json_path)
    print(f"[PROFIT] JSON 저장: {json_path}")

    # 텍스트 리포트 저장
    report_text = generate_report_text(month, result)
    report_path = root / "outputs" / "reports" / f"{month}_수익성리포트.txt"
    ensure_dir(report_path.parent)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"[PROFIT] 리포트 저장: {report_path}")

    print(f"[PROFIT] 완료 - {result['business_comparison'].get('summary', '')}")
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="PROFIT - 수익성 분석 에이전트"
    )
    parser.add_argument(
        "--month",
        type=str,
        default=datetime.now().strftime("%Y-%m"),
        help="분석 대상 월 (YYYY-MM, 기본: 이번 달)",
    )
    parser.add_argument(
        "--compare",
        nargs="+",
        choices=["workshop", "pension"],
        help="비교할 사업 (예: --compare workshop pension)",
    )
    args = parser.parse_args()
    run(month=args.month, compare=args.compare)


if __name__ == "__main__":
    main()
