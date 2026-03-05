"""
MARCO - Cost Analyst Agent
수주별 원가 분석 및 마진 모니터링

CNC 공방의 주문(수주)별 원가를 분석하여 재료비, 가공비, 인건비,
외주비, 간접비를 집계하고 마진율을 계산한다.
마진이 기준 이하인 수주에 대해 경고를 생성한다.
"""

import argparse
import glob
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir
from tools.calculator import calc_margin


# ---------------------------------------------------------------------------
# 데이터 로딩
# ---------------------------------------------------------------------------

def load_settings() -> Dict[str, Any]:
    """marco_settings.json을 로드한다. 없으면 기본값 반환."""
    root = get_project_root()
    settings_path = root / "data" / "config" / "marco_settings.json"
    if settings_path.exists():
        return load_json(settings_path)
    return {
        "hourly_machine_cost": {"cnc": 15000, "laser": 8000},
        "hourly_labor_cost": 15000,
        "indirect_cost_rate": 0.08,
        "min_margin_alert": 0.15,
        "critical_margin_alert": 0.05,
    }


def load_transactions(month: str) -> List[Dict[str, Any]]:
    """
    data/processed/transactions/ 에서 해당 월의 거래 데이터를 모두 로드한다.
    파일명 패턴: YYYY-MM*.json
    """
    root = get_project_root()
    tx_dir = root / "data" / "processed" / "transactions"
    if not tx_dir.exists():
        print(f"[MARCO] 거래 디렉터리 없음: {tx_dir}")
        return []

    transactions: List[Dict[str, Any]] = []
    patterns = [
        str(tx_dir / f"{month}*.json"),
        str(tx_dir / f"*{month}*.json"),
    ]
    seen_files: set = set()
    for pattern in patterns:
        for filepath in glob.glob(pattern):
            if filepath in seen_files:
                continue
            seen_files.add(filepath)
            try:
                data = load_json(filepath)
                if isinstance(data, list):
                    transactions.extend(data)
                elif isinstance(data, dict) and "transactions" in data:
                    transactions.extend(data["transactions"])
                elif isinstance(data, dict):
                    transactions.append(data)
            except Exception as e:
                print(f"[MARCO] 파일 로드 실패 {filepath}: {e}")
    return transactions


def load_orders(month: str) -> List[Dict[str, Any]]:
    """
    data/processed/orders/ 에서 해당 월의 수주 데이터를 로드한다.
    수주 데이터가 없으면 거래 데이터에서 '수주' 카테고리를 추출한다.
    """
    root = get_project_root()
    order_dir = root / "data" / "processed" / "orders"
    orders: List[Dict[str, Any]] = []

    if order_dir.exists():
        patterns = [
            str(order_dir / f"{month}*.json"),
            str(order_dir / f"*{month}*.json"),
        ]
        seen_files: set = set()
        for pattern in patterns:
            for filepath in glob.glob(pattern):
                if filepath in seen_files:
                    continue
                seen_files.add(filepath)
                try:
                    data = load_json(filepath)
                    if isinstance(data, list):
                        orders.extend(data)
                    elif isinstance(data, dict) and "orders" in data:
                        orders.extend(data["orders"])
                    elif isinstance(data, dict):
                        orders.append(data)
                except Exception as e:
                    print(f"[MARCO] 수주 파일 로드 실패 {filepath}: {e}")

    # 수주 데이터가 없으면 거래에서 추출 시도
    if not orders:
        transactions = load_transactions(month)
        orders = extract_orders_from_transactions(transactions)

    return orders


def extract_orders_from_transactions(transactions: List[Dict]) -> List[Dict]:
    """거래 데이터에서 '수주' 카테고리를 가진 항목을 수주로 변환."""
    orders = []
    for tx in transactions:
        category = str(tx.get("category", "")).lower()
        description = str(tx.get("description", "")).lower()
        if "수주" in category or "수주" in description or "order" in category:
            order = {
                "order_id": tx.get("order_id", tx.get("id", f"TX-{tx.get('date', 'unknown')}")),
                "date": tx.get("date", ""),
                "description": tx.get("description", tx.get("memo", "")),
                "revenue": int(tx.get("income", tx.get("amount", 0))),
                "material_type": tx.get("material_type", "기타"),
                "material_qty": float(tx.get("material_qty", 0)),
                "material_unit_price": int(tx.get("material_unit_price", 0)),
                "machine_type": tx.get("machine_type", "cnc"),
                "machine_hours": float(tx.get("machine_hours", 0)),
                "labor_hours": float(tx.get("labor_hours", 0)),
                "outsource_cost": int(tx.get("outsource_cost", 0)),
                "business_type": tx.get("business_type", "workshop"),
            }
            orders.append(order)
    return orders


# ---------------------------------------------------------------------------
# 원가 분석
# ---------------------------------------------------------------------------

def analyze_order_cost(order: Dict[str, Any], settings: Dict[str, Any]) -> Dict[str, Any]:
    """
    단일 수주의 원가를 분석한다.

    비용 항목:
        material_cost  = material_qty * material_unit_price
        machine_cost   = machine_hours * hourly_machine_cost[machine_type]
        labor_cost     = labor_hours * hourly_labor_cost
        outsource_cost = 외주비 (실비)
        indirect_cost  = revenue * indirect_cost_rate
        total_cost     = 위 합계
        profit         = revenue - total_cost
        margin_rate    = profit / revenue
    """
    hourly_machine = settings.get("hourly_machine_cost", {"cnc": 15000, "laser": 8000})
    hourly_labor = settings.get("hourly_labor_cost", 15000)
    indirect_rate = settings.get("indirect_cost_rate", 0.08)

    revenue = int(order.get("revenue", 0))
    material_qty = float(order.get("material_qty", 0))
    material_unit_price = int(order.get("material_unit_price", 0))
    machine_type = str(order.get("machine_type", "cnc")).lower()
    machine_hours = float(order.get("machine_hours", 0))
    labor_hours = float(order.get("labor_hours", 0))
    outsource = int(order.get("outsource_cost", 0))

    material_cost = int(material_qty * material_unit_price)
    machine_rate = hourly_machine.get(machine_type, hourly_machine.get("cnc", 15000))
    machine_cost = int(machine_hours * machine_rate)
    labor_cost = int(labor_hours * hourly_labor)
    indirect_cost = int(revenue * indirect_rate)
    total_cost = material_cost + machine_cost + labor_cost + outsource + indirect_cost
    profit = revenue - total_cost
    margin_rate = calc_margin(revenue, total_cost)

    return {
        "order_id": order.get("order_id", "unknown"),
        "date": order.get("date", ""),
        "description": order.get("description", ""),
        "revenue": revenue,
        "cost_breakdown": {
            "material_cost": material_cost,
            "machine_cost": machine_cost,
            "labor_cost": labor_cost,
            "outsource_cost": outsource,
            "indirect_cost": indirect_cost,
        },
        "total_cost": total_cost,
        "profit": profit,
        "margin_rate": round(margin_rate, 4),
        "machine_type": machine_type,
        "material_type": order.get("material_type", "기타"),
        "business_type": order.get("business_type", "workshop"),
    }


def compute_avg_margin_last_3months(month: str) -> float:
    """최근 3개월간의 마진 분석 파일에서 평균 마진율을 계산한다."""
    root = get_project_root()
    cost_dir = root / "data" / "processed" / "cost"
    if not cost_dir.exists():
        return 0.0

    # 최근 3개월 계산
    try:
        year, mon = map(int, month.split("-"))
    except ValueError:
        return 0.0

    margins: List[float] = []
    for offset in range(1, 4):
        m = mon - offset
        y = year
        while m <= 0:
            m += 12
            y -= 1
        prev_month = f"{y:04d}-{m:02d}"
        margin_file = cost_dir / f"{prev_month}_마진분석.json"
        if margin_file.exists():
            try:
                data = load_json(margin_file)
                if isinstance(data, dict) and "avg_margin_rate" in data:
                    margins.append(float(data["avg_margin_rate"]))
                elif isinstance(data, dict) and "orders" in data:
                    for o in data["orders"]:
                        if "margin_rate" in o:
                            margins.append(float(o["margin_rate"]))
            except Exception:
                pass

    return sum(margins) / len(margins) if margins else 0.0


# ---------------------------------------------------------------------------
# 마진 경고
# ---------------------------------------------------------------------------

def check_margin_alerts(
    analyzed_orders: List[Dict[str, Any]],
    settings: Dict[str, Any],
    month: str,
) -> List[Dict[str, Any]]:
    """
    마진율 기준 미달 수주를 검출하여 경고 리스트를 반환한다.
    - margin < min_margin_alert (15%): WARNING
    - margin < critical_margin_alert (5%): CRITICAL
    """
    min_margin = settings.get("min_margin_alert", 0.15)
    critical_margin = settings.get("critical_margin_alert", 0.05)
    avg_3m = compute_avg_margin_last_3months(month)

    alerts: List[Dict[str, Any]] = []
    for order in analyzed_orders:
        margin = order["margin_rate"]
        if margin < min_margin:
            severity = "CRITICAL" if margin < critical_margin else "WARNING"
            margin_pct = round(margin * 100, 1)
            avg_pct = round(avg_3m * 100, 1)
            message = (
                f"이 수주({order['order_id']})는 마진율 {margin_pct}%입니다. "
                f"최근 3개월 평균은 {avg_pct}%입니다."
            )
            alerts.append({
                "order_id": order["order_id"],
                "severity": severity,
                "margin_rate": margin,
                "margin_pct": margin_pct,
                "avg_3month_margin_pct": avg_pct,
                "message": message,
                "date": order.get("date", ""),
                "revenue": order["revenue"],
                "total_cost": order["total_cost"],
                "profit": order["profit"],
            })

    return alerts


# ---------------------------------------------------------------------------
# 장비 기여도 분석
# ---------------------------------------------------------------------------

def analyze_equipment_contribution(
    analyzed_orders: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """장비 유형별 매출 기여도 및 가동률을 계산한다."""
    equipment: Dict[str, Dict[str, float]] = {}

    for order in analyzed_orders:
        mtype = order.get("machine_type", "cnc")
        if mtype not in equipment:
            equipment[mtype] = {
                "total_revenue": 0,
                "total_machine_cost": 0,
                "total_hours": 0,
                "order_count": 0,
            }
        equipment[mtype]["total_revenue"] += order["revenue"]
        equipment[mtype]["total_machine_cost"] += order["cost_breakdown"]["machine_cost"]
        # machine_hours 역산: machine_cost / hourly_rate
        # 직접 order에서 읽는 것이 정확하므로 원본을 참조
        equipment[mtype]["order_count"] += 1

    # 월 가용 시간 (영업일 22일 * 8시간)
    monthly_available_hours = 22 * 8

    result: Dict[str, Any] = {}
    total_revenue = sum(e["total_revenue"] for e in equipment.values())

    for mtype, data in equipment.items():
        revenue_share = data["total_revenue"] / total_revenue if total_revenue > 0 else 0
        result[mtype] = {
            "total_revenue": int(data["total_revenue"]),
            "revenue_share": round(revenue_share, 4),
            "order_count": int(data["order_count"]),
            "total_machine_cost": int(data["total_machine_cost"]),
            "monthly_available_hours": monthly_available_hours,
        }

    return result


# ---------------------------------------------------------------------------
# 소재 단가 추적
# ---------------------------------------------------------------------------

def track_material_prices(
    analyzed_orders: List[Dict[str, Any]],
    transactions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    소재별(AL6061, SUS304, MDF 등) 단가 추이를 기록한다.
    거래 데이터와 수주 데이터에서 소재 정보를 추출한다.
    """
    target_materials = {"AL6061", "SUS304", "MDF"}
    material_prices: Dict[str, List[Dict[str, Any]]] = {}

    # 분석된 수주에서 소재 정보 추출
    for order in analyzed_orders:
        mat_type = order.get("material_type", "기타")
        if mat_type in target_materials or mat_type != "기타":
            if mat_type not in material_prices:
                material_prices[mat_type] = []
            cost_info = order.get("cost_breakdown", {})
            mat_cost = cost_info.get("material_cost", 0)
            # 추정 단가 (material_cost가 있고 수량 정보가 있는 경우)
            if mat_cost > 0:
                material_prices[mat_type].append({
                    "date": order.get("date", ""),
                    "order_id": order.get("order_id", ""),
                    "material_cost": mat_cost,
                })

    # 거래 데이터에서 소재 구매 내역 추출
    for tx in transactions:
        description = str(tx.get("description", "") or tx.get("memo", "")).upper()
        expense = int(tx.get("expense", tx.get("출금", 0)) or 0)
        if expense <= 0:
            continue

        for mat in target_materials:
            if mat.upper() in description or mat.lower() in description.lower():
                if mat not in material_prices:
                    material_prices[mat] = []
                material_prices[mat].append({
                    "date": tx.get("date", ""),
                    "expense": expense,
                    "description": tx.get("description", tx.get("memo", "")),
                })
                break

    # 요약 계산
    summary: Dict[str, Any] = {}
    for mat, records in material_prices.items():
        expenses = [r.get("expense", r.get("material_cost", 0)) for r in records if r.get("expense", r.get("material_cost", 0)) > 0]
        summary[mat] = {
            "records": records,
            "count": len(records),
            "total_cost": sum(expenses),
            "avg_cost": int(sum(expenses) / len(expenses)) if expenses else 0,
        }

    return summary


# ---------------------------------------------------------------------------
# 메인 실행 / 출력
# ---------------------------------------------------------------------------

def generate_sample_orders(month: str) -> List[Dict[str, Any]]:
    """
    실제 데이터가 없을 때 데모용 샘플 수주를 생성한다.
    실 운용 시에는 FELIX가 생성한 거래/수주 데이터를 사용한다.
    """
    try:
        year, mon = map(int, month.split("-"))
    except ValueError:
        year, mon = 2026, 3

    samples = [
        {
            "order_id": f"CNC-{year}-{mon:02d}1",
            "date": f"{year}-{mon:02d}-03",
            "description": "AL6061 정밀 가공 부품 50EA",
            "revenue": 2500000,
            "material_type": "AL6061",
            "material_qty": 12.5,
            "material_unit_price": 45000,
            "machine_type": "cnc",
            "machine_hours": 16,
            "labor_hours": 20,
            "outsource_cost": 0,
            "business_type": "workshop",
        },
        {
            "order_id": f"CNC-{year}-{mon:02d}2",
            "date": f"{year}-{mon:02d}-07",
            "description": "SUS304 브라켓 제작 30EA",
            "revenue": 1800000,
            "material_type": "SUS304",
            "material_qty": 8.0,
            "material_unit_price": 65000,
            "machine_type": "cnc",
            "machine_hours": 12,
            "labor_hours": 14,
            "outsource_cost": 100000,
            "business_type": "workshop",
        },
        {
            "order_id": f"CNC-{year}-{mon:02d}3",
            "date": f"{year}-{mon:02d}-12",
            "description": "MDF 레이저커팅 간판 10EA",
            "revenue": 500000,
            "material_type": "MDF",
            "material_qty": 5.0,
            "material_unit_price": 15000,
            "machine_type": "laser",
            "machine_hours": 4,
            "labor_hours": 3,
            "outsource_cost": 0,
            "business_type": "workshop",
        },
        {
            "order_id": f"CNC-{year}-{mon:02d}4",
            "date": f"{year}-{mon:02d}-18",
            "description": "AL6061 시제품 가공 5EA - 급행",
            "revenue": 350000,
            "material_type": "AL6061",
            "material_qty": 3.0,
            "material_unit_price": 45000,
            "machine_type": "cnc",
            "machine_hours": 8,
            "labor_hours": 10,
            "outsource_cost": 50000,
            "business_type": "workshop",
        },
        {
            "order_id": f"CNC-{year}-{mon:02d}5",
            "date": f"{year}-{mon:02d}-22",
            "description": "SUS304 정밀부품 외주포함 100EA",
            "revenue": 5000000,
            "material_type": "SUS304",
            "material_qty": 25.0,
            "material_unit_price": 65000,
            "machine_type": "cnc",
            "machine_hours": 40,
            "labor_hours": 35,
            "outsource_cost": 300000,
            "business_type": "workshop",
        },
    ]
    return samples


def run_marco(month: str, order_id: Optional[str] = None) -> None:
    """MARCO 에이전트 메인 실행 함수."""
    root = get_project_root()
    settings = load_settings()
    today_str = datetime.now().strftime("%Y%m%d")

    print(f"[MARCO] 원가 분석 시작 - 대상 월: {month}")
    if order_id:
        print(f"[MARCO] 대상 수주: {order_id}")

    # ── 1. 데이터 로드 ──
    orders = load_orders(month)
    transactions = load_transactions(month)

    if not orders:
        print("[MARCO] 수주 데이터 없음 - 샘플 데이터로 분석을 진행합니다.")
        orders = generate_sample_orders(month)

    # 특정 수주 필터
    if order_id:
        orders = [o for o in orders if o.get("order_id") == order_id]
        if not orders:
            print(f"[MARCO] 수주 {order_id}를 찾을 수 없습니다.")
            return

    print(f"[MARCO] 분석 대상 수주: {len(orders)}건")

    # ── 2. 수주별 원가 분석 ──
    analyzed_orders: List[Dict[str, Any]] = []
    for order in orders:
        result = analyze_order_cost(order, settings)
        analyzed_orders.append(result)
        margin_pct = round(result["margin_rate"] * 100, 1)
        print(
            f"  - {result['order_id']}: "
            f"매출 {result['revenue']:,}원 | "
            f"원가 {result['total_cost']:,}원 | "
            f"이익 {result['profit']:,}원 | "
            f"마진 {margin_pct}%"
        )

    # ── 3. 마진 경고 ──
    alerts = check_margin_alerts(analyzed_orders, settings, month)
    if alerts:
        print(f"\n[MARCO] 마진 경고 {len(alerts)}건 발생:")
        for alert in alerts:
            icon = "!!" if alert["severity"] == "CRITICAL" else "!"
            print(f"  {icon} [{alert['severity']}] {alert['message']}")

        # 경고 파일 저장
        alert_path = root / "outputs" / "alerts" / f"marco_low_margin_{today_str}.json"
        save_json(
            {
                "generated_at": datetime.now().isoformat(),
                "month": month,
                "alert_count": len(alerts),
                "alerts": alerts,
            },
            alert_path,
        )
        print(f"[MARCO] 경고 저장: {alert_path}")
    else:
        print("\n[MARCO] 마진 경고 없음 - 모든 수주가 기준 이상입니다.")

    # ── 4. 장비 기여도 분석 ──
    equipment = analyze_equipment_contribution(analyzed_orders)

    # ── 5. 소재 단가 추적 ──
    material_tracking = track_material_prices(analyzed_orders, transactions)

    # ── 6. 요약 통계 ──
    total_revenue = sum(o["revenue"] for o in analyzed_orders)
    total_cost = sum(o["total_cost"] for o in analyzed_orders)
    total_profit = sum(o["profit"] for o in analyzed_orders)
    avg_margin = calc_margin(total_revenue, total_cost) if total_revenue > 0 else 0.0

    summary = {
        "month": month,
        "generated_at": datetime.now().isoformat(),
        "order_count": len(analyzed_orders),
        "total_revenue": total_revenue,
        "total_cost": total_cost,
        "total_profit": total_profit,
        "avg_margin_rate": round(avg_margin, 4),
    }

    # ── 7. 결과 저장 ──
    cost_dir = root / "data" / "processed" / "cost"

    # 수주별 원가
    cost_path = cost_dir / f"{month}_수주별원가.json"
    save_json(
        {
            "month": month,
            "generated_at": datetime.now().isoformat(),
            "settings_used": settings,
            "orders": analyzed_orders,
            "equipment_contribution": equipment,
            "material_tracking": material_tracking,
        },
        cost_path,
    )
    print(f"\n[MARCO] 수주별 원가 저장: {cost_path}")

    # 마진 분석
    margin_path = cost_dir / f"{month}_마진분석.json"
    save_json(
        {
            **summary,
            "orders": [
                {
                    "order_id": o["order_id"],
                    "revenue": o["revenue"],
                    "total_cost": o["total_cost"],
                    "profit": o["profit"],
                    "margin_rate": o["margin_rate"],
                }
                for o in analyzed_orders
            ],
            "alerts": alerts,
        },
        margin_path,
    )
    print(f"[MARCO] 마진 분석 저장: {margin_path}")

    # ── 8. 최종 요약 출력 ──
    print(f"\n{'=' * 60}")
    print(f"  MARCO 원가 분석 요약 - {month}")
    print(f"{'=' * 60}")
    print(f"  분석 수주 수     : {len(analyzed_orders)}건")
    print(f"  총 매출          : {total_revenue:>12,}원")
    print(f"  총 원가          : {total_cost:>12,}원")
    print(f"  총 이익          : {total_profit:>12,}원")
    print(f"  평균 마진율      : {round(avg_margin * 100, 1):>10}%")
    print(f"  마진 경고        : {len(alerts)}건")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="MARCO - CNC 공방 수주별 원가 분석 에이전트",
    )
    parser.add_argument(
        "--month",
        type=str,
        default=datetime.now().strftime("%Y-%m"),
        help="분석 대상 월 (YYYY-MM, 기본: 이번 달)",
    )
    parser.add_argument(
        "--order",
        type=str,
        default=None,
        help="특정 수주 ID만 분석 (예: CNC-2026-047)",
    )
    args = parser.parse_args()

    run_marco(month=args.month, order_id=args.order)


if __name__ == "__main__":
    main()
