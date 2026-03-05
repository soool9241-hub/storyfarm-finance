"""
storyfarm-finance / agents / asset.py
ASSET - 자산·장비 관리 에이전트

공방 장비의 감가상각, 교체 시기, 정비 이력, 리스 vs 구매 분석을 담당한다.

읽는 데이터:
  - data/assets.json

출력:
  - data/processed/assets/YYYY-MM_자산현황.json
  - outputs/reports/자산관리보고서_YYYY-MM.txt
  - outputs/alerts/asset_replace_YYYYMMDD.json  (교체 경고 시)
"""

import sys
import os
import argparse
from datetime import datetime, date



sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir
from tools.calculator import calc_depreciation_straight


# ---------------------------------------------------------------------------
# 헬퍼
# ---------------------------------------------------------------------------

def _months_between_str(start_str: str, end_str: str) -> int:
    """두 날짜(YYYY-MM-DD) 사이의 개월 수를 반환한다."""
    try:
        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")
        return (end.year - start.year) * 12 + (end.month - start.month)
    except (ValueError, TypeError):
        return 0


def _format_won(amount: int) -> str:
    """금액을 한국 원화 형식으로 포맷."""
    return f"{amount:,}원"


# ---------------------------------------------------------------------------
# 1. 자산 등록 / 로드
# ---------------------------------------------------------------------------

def load_assets() -> list[dict]:
    """data/assets.json에서 모든 자산을 읽는다."""
    root = get_project_root()
    assets_path = root / "data" / "assets.json"
    if not assets_path.exists():
        print(f"[ASSET] 자산 파일 없음: {assets_path}")
        return []
    data = load_json(assets_path)
    return data.get("assets", [])


def save_assets(assets: list[dict]):
    """변경된 자산 목록을 data/assets.json에 저장한다."""
    root = get_project_root()
    assets_path = root / "data" / "assets.json"
    save_json({"assets": assets}, assets_path)
    print(f"[ASSET] 자산 파일 저장: {assets_path}")


# ---------------------------------------------------------------------------
# 2. 감가상각 계산
# ---------------------------------------------------------------------------

def calculate_depreciation(asset: dict, ref_date: str) -> dict:
    """자산 1건의 감가상각 현황을 계산한다.

    Args:
        asset: 자산 딕셔너리
        ref_date: 기준일 (YYYY-MM-DD)

    Returns:
        감가상각 상세 딕셔너리
    """
    cost = asset["acquisition_cost"]
    years = asset["useful_life_years"]
    rate = asset.get("residual_value_rate", 0.10)

    acquired = asset["acquired_date"]
    elapsed_months = _months_between_str(acquired, ref_date)
    total_life_months = years * 12

    # calc_depreciation_straight(cost, residual_rate, years, months_elapsed)
    dep = calc_depreciation_straight(cost, rate, years, elapsed_months)

    remaining_months = max(0, total_life_months - elapsed_months)
    life_elapsed_pct = round(
        min(elapsed_months / total_life_months, 1.0) * 100, 1
    ) if total_life_months > 0 else 100.0
    depreciation_complete = elapsed_months >= total_life_months

    residual_value = int(cost * rate)
    depreciable_amount = cost - residual_value

    return {
        "asset_id": asset["id"],
        "asset_name": asset["name"],
        "acquisition_cost": cost,
        "residual_value": residual_value,
        "depreciable_amount": depreciable_amount,
        "monthly_depreciation": dep["monthly_amount"],
        "annual_depreciation": dep["monthly_amount"] * 12,
        "elapsed_months": elapsed_months,
        "total_life_months": total_life_months,
        "remaining_months": remaining_months,
        "life_elapsed_pct": life_elapsed_pct,
        "accumulated_depreciation": dep["accumulated"],
        "book_value": dep["book_value"],
        "depreciation_complete": depreciation_complete,
    }


# ---------------------------------------------------------------------------
# 3. 교체 시기 판단
# ---------------------------------------------------------------------------

def evaluate_replacement(asset: dict, dep_info: dict) -> dict | None:
    """교체 시기 경고를 판단한다.

    조건:
      - 내용연수 80% 초과 경과: "교체 준비" 경고
      - 유지보수 누적 비용 > 취득가의 50%: "교체 검토" 경고

    Returns:
        경고 딕셔너리 또는 None
    """
    alerts = []

    # 내용연수 80% 이상 경과
    if dep_info["life_elapsed_pct"] >= 80:
        remaining = dep_info["remaining_months"]
        if dep_info["depreciation_complete"]:
            msg = f"{asset['name']}은(는) 내용연수가 종료되었습니다"
        else:
            msg = f"{asset['name']}은(는) {remaining}개월 후 내용연수 종료 예정입니다"
        alerts.append({
            "level": "교체 준비",
            "reason": "useful_life_80pct",
            "message": msg,
        })

    # 유지보수 누적 비용 > 취득가 50%
    maintenance_log = asset.get("maintenance_log", [])
    total_maintenance = sum(m.get("cost", 0) for m in maintenance_log)
    threshold = asset["acquisition_cost"] * 0.5
    if total_maintenance > threshold:
        alerts.append({
            "level": "교체 검토",
            "reason": "maintenance_cost_high",
            "message": (
                f"{asset['name']}의 유지보수 누적비용({_format_won(total_maintenance)})이 "
                f"취득가의 50%({_format_won(int(threshold))})를 초과했습니다"
            ),
            "total_maintenance_cost": total_maintenance,
        })

    if not alerts:
        return None

    return {
        "asset_id": asset["id"],
        "asset_name": asset["name"],
        "alerts": alerts,
    }


# ---------------------------------------------------------------------------
# 4. 정비 이력 관리
# ---------------------------------------------------------------------------

def add_maintenance(asset_id: str, cost: int, note: str,
                    vendor: str = "", date: str | None = None) -> bool:
    """자산에 정비 기록을 추가한다."""
    assets = load_assets()
    target = None
    for a in assets:
        if a["id"] == asset_id:
            target = a
            break

    if target is None:
        print(f"[ASSET] 자산 ID '{asset_id}'를 찾을 수 없습니다.")
        return False

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    entry = {
        "date": date,
        "cost": cost,
        "vendor": vendor,
        "note": note,
    }

    if "maintenance_log" not in target:
        target["maintenance_log"] = []
    target["maintenance_log"].append(entry)

    save_assets(assets)
    print(f"[ASSET] 정비 기록 추가 완료: {target['name']} - {note} ({_format_won(cost)})")
    return True


def analyze_maintenance(asset: dict, month: str | None = None) -> dict:
    """자산의 정비 이력을 분석한다.

    - 월별 정비 비용 합계
    - 정비 빈도 증가 패턴 감지
    """
    log = asset.get("maintenance_log", [])
    if not log:
        return {
            "asset_id": asset["id"],
            "total_entries": 0,
            "total_cost": 0,
            "monthly_summary": {},
            "frequency_increasing": False,
        }

    # 월별 그룹핑
    monthly: dict[str, list[dict]] = {}
    for entry in log:
        m = entry.get("date", "")[:7]  # YYYY-MM
        if m not in monthly:
            monthly[m] = []
        monthly[m].append(entry)

    monthly_summary = {}
    for m, entries in sorted(monthly.items()):
        monthly_summary[m] = {
            "count": len(entries),
            "cost": sum(e.get("cost", 0) for e in entries),
        }

    # 빈도 증가 패턴 감지: 최근 3개월의 건수가 이전 3개월보다 많으면
    sorted_months = sorted(monthly.keys())
    frequency_increasing = False
    if len(sorted_months) >= 6:
        recent_3 = sorted_months[-3:]
        prev_3 = sorted_months[-6:-3]
        recent_count = sum(len(monthly[m]) for m in recent_3)
        prev_count = sum(len(monthly[m]) for m in prev_3)
        if recent_count > prev_count:
            frequency_increasing = True
    elif len(sorted_months) >= 2:
        # 데이터가 적을 때: 마지막 달이 이전 달보다 건수 많으면
        last = sorted_months[-1]
        prev = sorted_months[-2]
        if len(monthly[last]) > len(monthly[prev]):
            frequency_increasing = True

    total_cost = sum(e.get("cost", 0) for e in log)

    result = {
        "asset_id": asset["id"],
        "asset_name": asset["name"],
        "total_entries": len(log),
        "total_cost": total_cost,
        "monthly_summary": monthly_summary,
        "frequency_increasing": frequency_increasing,
    }

    # 특정 월 필터
    if month and month in monthly_summary:
        result["current_month"] = {
            "month": month,
            **monthly_summary[month],
        }

    return result


# ---------------------------------------------------------------------------
# 5. 리스 vs 구매 분석
# ---------------------------------------------------------------------------

def analyze_lease_vs_buy(
    purchase_price: int,
    monthly_lease: int,
    useful_life_years: int = 5,
    residual_value_rate: float = 0.10,
    corporate_tax_rate: float = 0.20,
    compare_years: int = 5,
) -> dict:
    """리스 vs 구매의 5년 총비용을 비교한다.

    구매 시: 취득가 - 감가상각 세금 절감 혜택 + 잔존가치 회수
    리스 시: 월 리스료 * 기간

    Args:
        purchase_price: 구매 가격
        monthly_lease: 월 리스료
        useful_life_years: 내용연수
        residual_value_rate: 잔존가치율
        corporate_tax_rate: 법인세/소득세율 (절감 계산용)
        compare_years: 비교 기간 (기본 5년)
    """
    total_months = compare_years * 12

    # 리스 총비용
    lease_total = monthly_lease * total_months

    # 구매 총비용
    # calc_depreciation_straight(cost, residual_rate, years, months_elapsed)
    dep_months = min(useful_life_years * 12, total_months)
    dep = calc_depreciation_straight(
        purchase_price, residual_value_rate, useful_life_years, dep_months
    )
    monthly_dep = dep["monthly_amount"]
    total_depreciation = dep["accumulated"]

    # 감가상각에 의한 세금 절감
    tax_benefit = int(total_depreciation * corporate_tax_rate)

    # 잔존가치 (비교기간 후 남은 장부가)
    residual_value = int(purchase_price * residual_value_rate)
    if compare_years >= useful_life_years:
        residual_at_end = residual_value
    else:
        dep_partial = calc_depreciation_straight(
            purchase_price, residual_value_rate, useful_life_years, total_months
        )
        residual_at_end = dep_partial["book_value"]

    # 구매 실질 비용 = 취득가 - 세금 절감 - 잔존가치
    buy_net_cost = purchase_price - tax_benefit - residual_at_end

    # 판정
    if buy_net_cost < lease_total:
        recommendation = "구매"
        diff = lease_total - buy_net_cost
        summary = f"구매가 {_format_won(diff)} 유리합니다 ({compare_years}년 기준)"
    elif buy_net_cost > lease_total:
        recommendation = "리스"
        diff = buy_net_cost - lease_total
        summary = f"리스가 {_format_won(diff)} 유리합니다 ({compare_years}년 기준)"
    else:
        recommendation = "동일"
        diff = 0
        summary = f"구매와 리스의 {compare_years}년 총비용이 동일합니다"

    return {
        "purchase_price": purchase_price,
        "monthly_lease": monthly_lease,
        "compare_years": compare_years,
        "lease": {
            "monthly": monthly_lease,
            "total": lease_total,
        },
        "buy": {
            "acquisition_cost": purchase_price,
            "monthly_depreciation": monthly_dep,
            "total_depreciation": int(total_depreciation),
            "tax_benefit": tax_benefit,
            "residual_value_at_end": residual_at_end,
            "net_cost": buy_net_cost,
        },
        "recommendation": recommendation,
        "cost_difference": diff,
        "summary": summary,
    }


# ---------------------------------------------------------------------------
# 리포트 텍스트 생성
# ---------------------------------------------------------------------------

def generate_report_text(month: str, result: dict) -> str:
    """분석 결과를 텍스트 리포트로 변환한다."""
    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"  ASSET - {month} 자산관리 보고서")
    lines.append(f"  생성일시: {result.get('generated_at', '')}")
    lines.append(f"{'='*60}")
    lines.append("")

    # 자산 현황
    lines.append("[1] 자산 감가상각 현황")
    lines.append("-" * 50)
    for dep in result.get("depreciation", []):
        status = "[완료]" if dep["depreciation_complete"] else "[진행중]"
        lines.append(f"  {dep['asset_name']} ({dep['asset_id']}) {status}")
        lines.append(f"    취득가:      {dep['acquisition_cost']:>14,}원")
        lines.append(f"    월 상각액:   {dep['monthly_depreciation']:>14,}원")
        lines.append(f"    누적 상각:   {dep['accumulated_depreciation']:>14,}원")
        lines.append(f"    장부가:      {dep['book_value']:>14,}원")
        lines.append(f"    경과:        {dep['elapsed_months']}개월 / "
                      f"{dep['total_life_months']}개월 "
                      f"({dep['life_elapsed_pct']}%)")
        lines.append(f"    잔여:        {dep['remaining_months']}개월")
        lines.append("")

    # 교체 경고
    replacements = result.get("replacement_alerts", [])
    lines.append("[2] 교체 시기 경고")
    lines.append("-" * 50)
    if replacements:
        for r in replacements:
            for alert in r.get("alerts", []):
                lines.append(f"  [{alert['level']}] {alert['message']}")
    else:
        lines.append("  교체 필요 자산 없음")
    lines.append("")

    # 정비 이력 요약
    lines.append("[3] 정비 이력 요약")
    lines.append("-" * 50)
    for m_info in result.get("maintenance_analysis", []):
        a_name = m_info.get('asset_name', m_info.get('asset_id', '?'))
        lines.append(f"  {a_name} ({m_info.get('asset_id', '?')})")
        lines.append(f"    총 정비 횟수: {m_info['total_entries']}회")
        lines.append(f"    총 정비 비용: {m_info['total_cost']:>12,}원")
        if m_info.get("frequency_increasing"):
            lines.append("    [주의] 정비 빈도 증가 패턴 감지됨")
        ms = m_info.get("monthly_summary", {})
        if ms:
            for m_key in sorted(ms.keys()):
                v = ms[m_key]
                lines.append(
                    f"      {m_key}: {v['count']}회, {v['cost']:,}원"
                )
        lines.append("")

    # 총 자산 요약
    deps = result.get("depreciation", [])
    total_cost = sum(d["acquisition_cost"] for d in deps)
    total_book = sum(d["book_value"] for d in deps)
    total_accum = sum(d["accumulated_depreciation"] for d in deps)

    lines.append("[4] 자산 총괄 요약")
    lines.append("-" * 50)
    lines.append(f"  총 자산 수:    {len(deps)}건")
    lines.append(f"  총 취득가:     {total_cost:>14,}원")
    lines.append(f"  총 누적상각:   {total_accum:>14,}원")
    lines.append(f"  총 장부가:     {total_book:>14,}원")
    lines.append("")

    lines.append(f"{'='*60}")
    lines.append("  End of Report")
    lines.append(f"{'='*60}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 메인 실행
# ---------------------------------------------------------------------------

def run(month: str):
    """ASSET 에이전트 메인 로직."""
    root = get_project_root()
    today = datetime.now().strftime("%Y-%m-%d")
    # 기준일: 해당 월의 마지막 날
    try:
        year, mon = map(int, month.split("-"))
        if mon == 12:
            ref_date = f"{year + 1}-01-01"
        else:
            ref_date = f"{year}-{mon + 1:02d}-01"
        # 하루 전 = 해당 월 마지막 날
        from datetime import timedelta
        ref_dt = datetime.strptime(ref_date, "%Y-%m-%d") - timedelta(days=1)
        ref_date = ref_dt.strftime("%Y-%m-%d")
    except Exception:
        ref_date = today

    print(f"[ASSET] {month} 자산 현황 분석 시작 (기준일: {ref_date})")

    assets = load_assets()
    if not assets:
        print("[ASSET] 등록된 자산이 없습니다.")
        return {}

    print(f"[ASSET] 자산 {len(assets)}건 로드됨")

    result = {
        "month": month,
        "ref_date": ref_date,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "depreciation": [],
        "replacement_alerts": [],
        "maintenance_analysis": [],
    }

    # 각 자산별 처리
    for asset in assets:
        # 감가상각
        dep_info = calculate_depreciation(asset, ref_date)
        result["depreciation"].append(dep_info)

        if dep_info["depreciation_complete"]:
            print(f"  [{asset['id']}] {asset['name']}: 감가상각 완료")
        else:
            print(f"  [{asset['id']}] {asset['name']}: "
                  f"장부가 {_format_won(dep_info['book_value'])}, "
                  f"잔여 {dep_info['remaining_months']}개월")

        # 교체 시기 판단
        replacement = evaluate_replacement(asset, dep_info)
        if replacement:
            result["replacement_alerts"].append(replacement)
            for alert in replacement["alerts"]:
                print(f"  [경고] {alert['message']}")

        # 정비 이력 분석
        maint = analyze_maintenance(asset, month)
        result["maintenance_analysis"].append(maint)

    # JSON 저장
    json_path = root / "data" / "processed" / "assets" / f"{month}_자산현황.json"
    save_json(result, json_path)
    print(f"\n[ASSET] JSON 저장: {json_path}")

    # 텍스트 리포트 저장
    report_text = generate_report_text(month, result)
    report_path = root / "outputs" / "reports" / f"자산관리보고서_{month}.txt"
    ensure_dir(report_path.parent)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text)
    print(f"[ASSET] 리포트 저장: {report_path}")

    # 교체 경고 alerts 저장
    if result["replacement_alerts"]:
        alert_date = datetime.now().strftime("%Y%m%d")
        alert_path = (root / "outputs" / "alerts"
                      / f"asset_replace_{alert_date}.json")
        save_json({
            "generated_at": result["generated_at"],
            "alerts": result["replacement_alerts"],
        }, alert_path)
        print(f"[ASSET] 교체 경고 저장: {alert_path}")

    print(f"[ASSET] 완료 - 자산 {len(assets)}건 분석됨")
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ASSET - 자산·장비 관리 에이전트"
    )
    parser.add_argument(
        "--month",
        type=str,
        default=datetime.now().strftime("%Y-%m"),
        help="분석 대상 월 (YYYY-MM, 기본: 이번 달)",
    )
    parser.add_argument(
        "--add-maintenance",
        type=str,
        metavar="ASSET_ID",
        help="정비 기록 추가 대상 자산 ID (예: ASSET-001)",
    )
    parser.add_argument(
        "--cost",
        type=int,
        help="정비 비용 (--add-maintenance와 함께 사용)",
    )
    parser.add_argument(
        "--note",
        type=str,
        default="",
        help="정비 메모 (--add-maintenance와 함께 사용)",
    )
    parser.add_argument(
        "--vendor",
        type=str,
        default="",
        help="정비 업체 (--add-maintenance와 함께 사용)",
    )
    parser.add_argument(
        "--analyze-lease-vs-buy",
        action="store_true",
        help="리스 vs 구매 비교 분석 실행",
    )
    parser.add_argument(
        "--price",
        type=int,
        help="구매 가격 (--analyze-lease-vs-buy와 함께 사용)",
    )
    parser.add_argument(
        "--monthly-lease",
        type=int,
        help="월 리스료 (--analyze-lease-vs-buy와 함께 사용)",
    )

    args = parser.parse_args()

    # 정비 기록 추가 모드
    if args.add_maintenance:
        if args.cost is None:
            parser.error("--cost는 --add-maintenance와 함께 필수입니다.")
        add_maintenance(
            asset_id=args.add_maintenance,
            cost=args.cost,
            note=args.note,
            vendor=args.vendor,
        )
        return

    # 리스 vs 구매 분석 모드
    if args.analyze_lease_vs_buy:
        if args.price is None or args.monthly_lease is None:
            parser.error(
                "--price와 --monthly-lease는 "
                "--analyze-lease-vs-buy와 함께 필수입니다."
            )
        result = analyze_lease_vs_buy(
            purchase_price=args.price,
            monthly_lease=args.monthly_lease,
        )
        print(f"\n{'='*50}")
        print("  리스 vs 구매 분석 결과")
        print(f"{'='*50}")
        print(f"  구매 가격:       {result['purchase_price']:>12,}원")
        print(f"  월 리스료:       {result['monthly_lease']:>12,}원")
        print(f"  비교 기간:       {result['compare_years']}년")
        print(f"{'─'*50}")
        print(f"  리스 총비용:     {result['lease']['total']:>12,}원")
        print(f"  구매 실질비용:   {result['buy']['net_cost']:>12,}원")
        print(f"    (취득가 {result['buy']['acquisition_cost']:,}원"
              f" - 세금절감 {result['buy']['tax_benefit']:,}원"
              f" - 잔존가 {result['buy']['residual_value_at_end']:,}원)")
        print(f"{'─'*50}")
        print(f"  추천: {result['recommendation']}")
        print(f"  >> {result['summary']}")
        print(f"{'='*50}")

        # 결과 JSON도 저장
        root = get_project_root()
        lv_path = (root / "data" / "processed" / "assets"
                    / f"lease_vs_buy_{datetime.now().strftime('%Y%m%d')}.json")
        save_json(result, lv_path)
        print(f"\n[ASSET] 분석 결과 저장: {lv_path}")
        return

    # 기본 모드: 월간 자산 현황 분석
    run(month=args.month)


if __name__ == "__main__":
    main()
