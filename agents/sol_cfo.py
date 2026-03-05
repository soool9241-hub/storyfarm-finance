"""
agents/sol_cfo.py - SOL-CFO: Chief Financial Officer (Commander)
전체 에이전트를 오케스트레이션하고 경영진 브리핑을 제공한다.
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import (
    save_json,
    load_json,
    get_project_root,
    ensure_dir,
    print_briefing,
    print_separator,
)


# ── 로컬 헬퍼 ────────────────────────────────────────────────

def format_krw(amount) -> str:
    """금액을 한국 원화 형식으로 포맷. 예: 1,234,567원"""
    try:
        return f"{int(amount):,}원"
    except (ValueError, TypeError):
        return "0원"


# ── 경로 상수 ────────────────────────────────────────────────

ROOT = get_project_root()
AGENTS_DIR = ROOT / "agents"
PROCESSED = ROOT / "data" / "processed"
ALERTS_DIR = ROOT / "outputs" / "alerts"
REPORTS_DIR = ROOT / "outputs" / "reports"
BRIEFING_DIR = ROOT / "outputs" / "briefing"

PYTHON = sys.executable


# ── 에이전트 실행 헬퍼 ────────────────────────────────────────

def _agent_path(name: str) -> str:
    """에이전트 스크립트의 절대 경로를 반환."""
    return str(AGENTS_DIR / f"{name}.py")


def _run_sequential(agent_name: str, args: list = None) -> bool:
    """에이전트를 순차 실행하고 성공 여부를 반환."""
    cmd = [PYTHON, _agent_path(agent_name)]
    if args:
        cmd.extend(args)

    agent_file = Path(_agent_path(agent_name))
    if not agent_file.exists():
        print(f"  [SKIP] {agent_name}.py 파일 없음 - 건너뜀")
        return True

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(ROOT),
            encoding="utf-8",
            errors="replace",
        )
        if result.returncode == 0:
            print(f"  [OK] {agent_name} 완료")
            if result.stdout.strip():
                for line in result.stdout.strip().split("\n")[-5:]:
                    print(f"    {line}")
            return True
        else:
            print(f"  [FAIL] {agent_name} 실패 (exit code: {result.returncode})")
            if result.stderr.strip():
                for line in result.stderr.strip().split("\n")[-3:]:
                    print(f"    ERR: {line}")
            return False
    except subprocess.TimeoutExpired:
        print(f"  [TIMEOUT] {agent_name} 타임아웃 (300초 초과)")
        return False
    except Exception as e:
        print(f"  [ERROR] {agent_name} 실행 오류: {e}")
        return False


def _run_parallel(agent_configs: list) -> dict:
    """
    여러 에이전트를 병렬 실행(subprocess.Popen)하고 결과를 반환.

    Args:
        agent_configs: [(agent_name, [args]), ...] 형태의 리스트

    Returns:
        {agent_name: bool} 성공 여부 딕셔너리
    """
    processes = {}
    results = {}

    for agent_name, args in agent_configs:
        agent_file = Path(_agent_path(agent_name))
        if not agent_file.exists():
            print(f"  [SKIP] {agent_name}.py 파일 없음 - 건너뜀")
            results[agent_name] = True
            continue

        cmd = [PYTHON, _agent_path(agent_name)]
        if args:
            cmd.extend(args)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(ROOT),
                encoding="utf-8",
                errors="replace",
            )
            processes[agent_name] = proc
        except Exception as e:
            print(f"  [ERROR] {agent_name} 시작 실패: {e}")
            results[agent_name] = False

    # 모든 프로세스 완료 대기
    for agent_name, proc in processes.items():
        try:
            stdout, stderr = proc.communicate(timeout=300)
            if proc.returncode == 0:
                print(f"  [OK] {agent_name} 완료")
                results[agent_name] = True
            else:
                print(f"  [FAIL] {agent_name} 실패 (exit code: {proc.returncode})")
                if stderr.strip():
                    for line in stderr.strip().split("\n")[-3:]:
                        print(f"    ERR: {line}")
                results[agent_name] = False
        except subprocess.TimeoutExpired:
            proc.kill()
            print(f"  [TIMEOUT] {agent_name} 타임아웃")
            results[agent_name] = False

    return results


# ── 1. 오케스트레이션 ─────────────────────────────────────────

def run_monthly_close(month: str) -> dict:
    """
    월간 마감 프로세스: 전체 6개 Phase 실행.
    Phase 1: felix (데이터 수집/정제)
    Phase 2: luna + marco + asset (병렬)
    Phase 3: cash (순차, Phase 2 의존)
    Phase 4: debt + tax + profit (병렬)
    Phase 5: report (순차, 전체 의존)
    Phase 6: 최종 브리핑 생성
    """
    month_args = ["--month", month]
    phase_results = {}

    # Phase 1
    print("\nPhase 1 시작... (데이터 수집/정제)")
    phase_results["phase1"] = _run_sequential("felix", month_args)
    print("Phase 1 완료\n")

    # Phase 2
    print("Phase 2 시작... (회계/원가/자산 - 병렬)")
    p2 = _run_parallel([
        ("luna", month_args),
        ("marco", month_args),
        ("asset", month_args),
    ])
    phase_results["phase2"] = all(p2.values())
    print("Phase 2 완료\n")

    # Phase 3
    print("Phase 3 시작... (현금흐름)")
    phase_results["phase3"] = _run_sequential("cash", ["--days", "30", "--scenario", "base"])
    print("Phase 3 완료\n")

    # Phase 4
    print("Phase 4 시작... (채무/세무/수익성 - 병렬)")
    quarter = f"{month[:4]}-Q{(int(month[5:7]) - 1) // 3 + 1}"
    p4 = _run_parallel([
        ("debt", ["--simulate"]),
        ("tax", ["--quarter", quarter, "--prepare"]),
        ("profit", month_args),
    ])
    phase_results["phase4"] = all(p4.values())
    print("Phase 4 완료\n")

    # Phase 5
    print("Phase 5 시작... (보고서 생성)")
    phase_results["phase5"] = _run_sequential(
        "report", ["--month", month, "--full"]
    )
    print("Phase 5 완료\n")

    # Phase 6
    print("Phase 6 시작... (최종 브리핑)")
    briefing = generate_executive_briefing(month)
    phase_results["phase6"] = briefing is not None
    print("Phase 6 완료\n")

    # 최종 요약
    total = len(phase_results)
    passed = sum(1 for v in phase_results.values() if v)
    print_separator("=", 50)
    print(f"  월간 마감 완료: {passed}/{total} Phase 성공")
    print_separator("=", 50)

    return phase_results


def run_daily(month: str = None) -> dict:
    """일일 점검 모드: 핵심 에이전트만 실행하고 브리핑."""
    if month is None:
        month = datetime.now().strftime("%Y-%m")

    month_args = ["--month", month]

    print("\n일일 점검 시작...\n")

    print("[1/4] 데이터 갱신...")
    _run_sequential("felix", month_args)

    print("[2/4] 현금 현황 갱신...")
    _run_sequential("cash", ["--days", "30", "--scenario", "base"])

    print("[3/4] 채무 현황 갱신...")
    _run_sequential("debt", ["--simulate"])

    print("[4/4] 브리핑 생성...")
    briefing = generate_executive_briefing(month)

    return {"status": "daily_complete", "briefing": briefing}


# ── 2. 알림 우선순위 분류 ─────────────────────────────────────

def load_all_alerts() -> list:
    """outputs/alerts/ 내의 모든 JSON 알림을 로드."""
    alerts = []
    if not ALERTS_DIR.exists():
        return alerts
    for fpath in sorted(ALERTS_DIR.glob("*.json")):
        try:
            data = load_json(str(fpath))
            if isinstance(data, list):
                alerts.extend(data)
            elif isinstance(data, dict):
                alerts.append(data)
        except Exception:
            pass
    return alerts


def prioritize_alerts(alerts: list) -> dict:
    """
    알림을 3단계 우선순위로 분류한다.

    RED  (즉시 조치): D-day <= 7, balance danger, margin < 5%
    YELLOW (이번주 내): D-day <= 30, margin < 15%
    GREEN (이번달 내): 일반 정보
    """
    red = []
    yellow = []
    green = []
    today = datetime.now()

    for alert in alerts:
        priority = str(alert.get("priority", alert.get("우선순위", "")))
        level = str(alert.get("level", alert.get("등급", "")))
        msg = alert.get("message", alert.get("내용", alert.get("alert", "")))

        # D-day 계산
        due_date = alert.get("due_date", alert.get("기한", alert.get("date", "")))
        d_day = None
        if due_date:
            try:
                due = datetime.strptime(str(due_date)[:10], "%Y-%m-%d")
                d_day = (due - today).days
            except ValueError:
                pass

        # 마진율
        margin = alert.get("margin", alert.get("마진율", None))
        if margin is not None:
            try:
                margin = float(margin)
            except (ValueError, TypeError):
                margin = None

        # 잔액 위험 플래그
        balance_danger = alert.get("balance_danger", alert.get("잔액위험", False))

        # ── 분류 ──
        is_red = False
        is_yellow = False

        # RED 조건
        if d_day is not None and d_day <= 7:
            is_red = True
        if balance_danger:
            is_red = True
        if margin is not None and margin < 5:
            is_red = True
        if priority.lower() in ("high", "critical", "긴급", "즉시"):
            is_red = True
        if level.lower() in ("danger", "critical", "위험"):
            is_red = True

        # YELLOW 조건
        if not is_red:
            if d_day is not None and d_day <= 30:
                is_yellow = True
            if margin is not None and margin < 15:
                is_yellow = True
            if priority.lower() in ("medium", "중간", "주의"):
                is_yellow = True
            if level.lower() in ("warning", "주의"):
                is_yellow = True

        entry = {"message": msg, "d_day": d_day, "margin": margin, "raw": alert}

        if is_red:
            red.append(entry)
        elif is_yellow:
            yellow.append(entry)
        else:
            green.append(entry)

    # D-day 기준 정렬
    for lst in [red, yellow, green]:
        lst.sort(key=lambda x: x["d_day"] if x["d_day"] is not None else 999)

    return {"red": red, "yellow": yellow, "green": green}


def format_prioritized_alerts(prioritized: dict) -> str:
    """우선순위 분류된 알림을 포맷 문자열로 반환."""
    lines = []

    if prioritized["red"]:
        lines.append("[RED] 즉시 조치 필요:")
        for a in prioritized["red"]:
            d_str = f" (D-{a['d_day']})" if a["d_day"] is not None else ""
            lines.append(f"  - {a['message']}{d_str}")
    else:
        lines.append("[RED] 즉시 조치 항목 없음")

    lines.append("")

    if prioritized["yellow"]:
        lines.append("[YELLOW] 이번주 내 확인:")
        for a in prioritized["yellow"]:
            d_str = f" (D-{a['d_day']})" if a["d_day"] is not None else ""
            lines.append(f"  - {a['message']}{d_str}")
    else:
        lines.append("[YELLOW] 주의 항목 없음")

    lines.append("")

    if prioritized["green"]:
        lines.append("[GREEN] 이번달 내 참고:")
        for a in prioritized["green"][:5]:
            lines.append(f"  - {a['message']}")
        if len(prioritized["green"]) > 5:
            lines.append(f"  ... 외 {len(prioritized['green']) - 5}건")
    else:
        lines.append("[GREEN] 참고 항목 없음")

    return "\n".join(lines)


# ── 3. 경영진 브리핑 (3-line summary) ─────────────────────────

def _load_report_json(month: str, report_type: str) -> dict:
    """reports 디렉토리에서 JSON 보고서를 로드."""
    filenames = {
        "income": f"{month}_income_statement.json",
        "balance": f"{month}_balance_sheet.json",
        "cash_flow": f"{month}_cash_flow.json",
    }
    fname = filenames.get(report_type, "")
    fpath = REPORTS_DIR / fname
    if fpath.exists():
        try:
            return load_json(str(fpath))
        except Exception:
            pass
    return {}


def _load_processed_data(subdir: str) -> list:
    """data/processed/{subdir}/ 의 모든 JSON을 로드."""
    target_dir = PROCESSED / subdir
    results = []
    if not target_dir.exists():
        return results
    for fpath in sorted(target_dir.glob("*.json")):
        try:
            data = load_json(str(fpath))
            if isinstance(data, list):
                results.extend(data)
            elif isinstance(data, dict):
                results.append(data)
        except Exception:
            pass
    return results


def generate_executive_briefing(month: str) -> str:
    """경영진 브리핑을 생성하고 저장한다."""
    today = datetime.now()

    # 보고서 데이터 로드 (파일 없으면 직접 생성)
    income = _load_report_json(month, "income")
    balance = _load_report_json(month, "balance")
    cash_flow = _load_report_json(month, "cash_flow")

    if not income:
        try:
            from agents.report import generate_income_statement
            income = generate_income_statement(month)
        except Exception:
            income = {}

    if not balance:
        try:
            from agents.report import generate_balance_sheet
            balance = generate_balance_sheet(month)
        except Exception:
            balance = {}

    if not cash_flow:
        try:
            from agents.report import generate_cash_flow
            cash_flow = generate_cash_flow(month)
        except Exception:
            cash_flow = {}

    # 핵심 지표 추출
    total_revenue = 0
    op_income = 0
    op_margin = 0.0
    if income:
        total_revenue = income.get("revenue", {}).get("매출액_합계", 0)
        op_income = income.get("영업이익", 0)
        op_margin = (op_income / total_revenue * 100) if total_revenue > 0 else 0.0

    current_balance = 0
    if balance:
        current_balance = (
            balance.get("assets", {})
            .get("유동자산", {})
            .get("현금_보통예금", 0)
        )

    # N일 후 예상 잔고
    days_left = max(0, 30 - today.day)
    daily_cf = 0
    if cash_flow:
        monthly_net = cash_flow.get("현금증감", 0)
        daily_cf = monthly_net / 30 if monthly_net else 0
    projected_balance = current_balance + int(daily_cf * days_left)

    # 알림 요약
    alerts = load_all_alerts()
    prioritized = prioritize_alerts(alerts)
    top_alert = "특이사항 없음"
    if prioritized["red"]:
        top_alert = prioritized["red"][0]["message"]
    elif prioritized["yellow"]:
        top_alert = prioritized["yellow"][0]["message"]

    # 브리핑 텍스트 조립
    sep = "\u2501" * 35  # ━

    briefing_lines = [
        sep,
        f"솔님, {today.month}월 {today.day}일 재무 현황이에요",
        sep,
        f"이번달 매출 {format_krw(total_revenue)} / "
        f"영업이익 {format_krw(op_income)} ({op_margin:.1f}%)",
        f"현재 잔고 {format_krw(current_balance)} -> "
        f"{days_left}일 후 {format_krw(projected_balance)} 예상",
        f"지금 봐야 할 것: {top_alert}",
        sep,
    ]

    briefing_text = "\n".join(briefing_lines)
    print()
    print(briefing_text)

    # 상세 알림 출력
    print()
    alert_text = format_prioritized_alerts(prioritized)
    print(alert_text)

    # 브리핑 JSON 저장
    ensure_dir(str(BRIEFING_DIR))
    briefing_data = {
        "generated_at": today.strftime("%Y-%m-%d %H:%M:%S"),
        "month": month,
        "summary": {
            "total_revenue": total_revenue,
            "operating_income": op_income,
            "operating_margin_pct": round(op_margin, 1),
            "current_balance": current_balance,
            "projected_balance": projected_balance,
            "days_left": days_left,
            "top_alert": top_alert,
        },
        "alerts": {
            "red_count": len(prioritized["red"]),
            "yellow_count": len(prioritized["yellow"]),
            "green_count": len(prioritized["green"]),
            "details": {
                "red": [a["message"] for a in prioritized["red"]],
                "yellow": [a["message"] for a in prioritized["yellow"]],
                "green": [a["message"] for a in prioritized["green"]],
            },
        },
    }

    save_json(briefing_data, str(BRIEFING_DIR / f"{month}_briefing.json"))
    save_json(
        {"text": briefing_text, "alerts": alert_text},
        str(BRIEFING_DIR / f"{month}_briefing_text.json"),
    )

    return briefing_text


# ── 4. 자연어 Q&A ────────────────────────────────────────────

QA_KEYWORD_MAP = {
    "debt": {
        "keywords": ["카드론", "대출", "차입", "상환", "빚", "채무", "갚"],
        "data_source": "debt",
        "description": "채무/대출 현황",
    },
    "cost": {
        "keywords": ["수주", "마진", "원가", "단가", "이익률", "남았", "남는"],
        "data_source": "marco",
        "description": "원가/마진 분석",
    },
    "profit": {
        "keywords": ["매출", "이익", "수익", "실적", "괜찮", "좋", "나쁘", "손익"],
        "data_source": "profit",
        "description": "매출/수익성 분석",
    },
    "tax": {
        "keywords": ["세금", "세무", "부가세", "부가가치", "원천세", "종소세", "납부"],
        "data_source": "tax",
        "description": "세무 현황",
    },
    "cash": {
        "keywords": ["현금", "잔고", "통장", "돈", "자금", "잔액"],
        "data_source": "cash",
        "description": "현금/자금 현황",
    },
    "asset": {
        "keywords": ["설비", "기계", "장비", "자산", "감가"],
        "data_source": "asset",
        "description": "자산/설비 현황",
    },
}


def detect_question_topic(question: str) -> list:
    """질문 키워드를 감지하여 관련 토픽 리스트를 반환."""
    topics = []
    for topic, config in QA_KEYWORD_MAP.items():
        for kw in config["keywords"]:
            if kw in question:
                topics.append(topic)
                break
    return topics if topics else ["profit"]


def answer_question(question: str, month: str) -> str:
    """자연어 질문에 답변한다. 키워드 기반으로 데이터 소스를 결정."""
    topics = detect_question_topic(question)

    print(f"\n질문: {question}")
    print(f"감지된 토픽: {', '.join(topics)}\n")

    answer_parts = []

    for topic in topics:
        source = QA_KEYWORD_MAP.get(topic, {}).get("data_source", "")
        data = _load_processed_data(source)
        income = _load_report_json(month, "income")
        balance = _load_report_json(month, "balance")

        if topic == "debt":
            answer_parts.append(_answer_debt(data, question))
        elif topic == "cost":
            answer_parts.append(_answer_cost(data, question))
        elif topic == "profit":
            answer_parts.append(_answer_profit(data, income, question))
        elif topic == "tax":
            answer_parts.append(_answer_tax(data, question))
        elif topic == "cash":
            answer_parts.append(_answer_cash(data, balance, question))
        elif topic == "asset":
            answer_parts.append(_answer_asset(data, question))

    if not answer_parts:
        answer_parts.append(
            "관련 데이터를 찾을 수 없습니다. 월간 마감을 먼저 실행해 주세요."
        )

    full_answer = "\n\n".join(answer_parts)

    sep = "\u2501" * 35
    print(sep)
    print(full_answer)
    print(sep)

    return full_answer


def _answer_debt(data: list, question: str) -> str:
    """채무 관련 질문에 대한 답변을 생성."""
    if not data:
        return "[채무] 채무 데이터가 없습니다. debt.py를 먼저 실행해 주세요."

    lines = ["[채무 현황]"]
    total_debt = 0

    for r in data:
        name = r.get("name", r.get("대출명", "대출"))
        remaining = 0
        try:
            remaining = int(float(
                r.get("remaining_balance", r.get("잔액", 0))
            ))
            total_debt += remaining
        except (ValueError, TypeError):
            pass

        monthly = r.get("monthly_payment", r.get("상환액", 0))
        maturity = r.get("maturity_date", r.get("만기일", "미정"))

        lines.append(
            f"  - {name}: 잔액 {format_krw(remaining)}, "
            f"월 상환 {format_krw(monthly)}"
        )
        if maturity:
            lines.append(f"    만기일: {maturity}")

        # "언제 다 갚아?" 유형 질문
        if any(kw in question for kw in ["언제", "다 갚", "완납", "끝"]):
            try:
                monthly_val = int(float(monthly))
                if monthly_val > 0 and remaining > 0:
                    months_left = remaining // monthly_val
                    years = months_left // 12
                    mons = months_left % 12
                    lines.append(
                        f"    -> 약 {years}년 {mons}개월 후 완납 예상 "
                        f"({months_left}개월)"
                    )
            except (ValueError, TypeError):
                pass

    lines.append(f"\n  채무 총액: {format_krw(total_debt)}")
    return "\n".join(lines)


def _answer_cost(data: list, question: str) -> str:
    """원가/마진 관련 질문에 대한 답변을 생성."""
    if not data:
        return "[원가] 원가 데이터가 없습니다. marco.py를 먼저 실행해 주세요."

    lines = ["[수주별 마진 분석]"]

    sorted_data = sorted(
        data,
        key=lambda x: float(x.get("margin", x.get("마진율", 0)) or 0),
        reverse=True,
    )

    for r in sorted_data[:10]:
        name = r.get("project", r.get("수주명", r.get("name", "?")))
        revenue = r.get("revenue", r.get("매출", 0))
        profit = r.get("profit", r.get("이익", 0))
        margin = r.get("margin", r.get("마진율", 0))

        try:
            margin_pct = (
                float(margin) * 100 if float(margin) <= 1 else float(margin)
            )
        except (ValueError, TypeError):
            margin_pct = 0.0

        lines.append(
            f"  - {name}: 매출 {format_krw(revenue)}, "
            f"마진 {margin_pct:.1f}%, 이익 {format_krw(profit)}"
        )

    if sorted_data:
        best_name = sorted_data[0].get(
            "project", sorted_data[0].get("수주명", sorted_data[0].get("name", "?"))
        )
        lines.append(f"\n  가장 마진이 높은 수주: {best_name}")

    return "\n".join(lines)


def _answer_profit(data: list, income: dict, question: str) -> str:
    """매출/수익 관련 질문에 대한 답변을 생성."""
    lines = ["[매출/수익 현황]"]

    if income:
        rev = income.get("revenue", {}).get("매출액_합계", 0)
        op = income.get("영업이익", 0)
        margin = (op / rev * 100) if rev > 0 else 0

        lines.append(f"  매출: {format_krw(rev)}")
        lines.append(f"  영업이익: {format_krw(op)} (이익률 {margin:.1f}%)")

        if any(kw in question for kw in ["괜찮", "좋", "어때", "상황"]):
            if margin >= 20:
                lines.append(
                    "  -> 양호합니다! 이익률 20% 이상으로 건강한 수준이에요."
                )
            elif margin >= 10:
                lines.append(
                    "  -> 보통 수준입니다. 이익률 관리에 신경 써주세요."
                )
            elif margin >= 0:
                lines.append(
                    "  -> 주의가 필요합니다. 이익률이 낮은 편이에요."
                )
            else:
                lines.append(
                    "  -> 적자 상태입니다. 비용 절감이나 매출 증대가 시급해요."
                )
    else:
        lines.append("  손익 데이터가 없습니다. report.py를 먼저 실행해 주세요.")

    if data:
        lines.append("\n  [상세 수익 데이터]")
        for r in data[:5]:
            btype = r.get("business_type", "")
            rev = r.get("revenue", r.get("매출", 0))
            lines.append(f"  - {btype}: {format_krw(rev)}")

    return "\n".join(lines)


def _answer_tax(data: list, question: str) -> str:
    """세무 관련 질문에 대한 답변을 생성."""
    if not data:
        return "[세무] 세무 데이터가 없습니다. tax.py를 먼저 실행해 주세요."

    lines = ["[세무 현황]"]
    for r in data[:10]:
        tax_type = r.get("tax_type", r.get("세목", "세금"))
        amount = r.get("amount", r.get("세액", 0))
        due = r.get("due_date", r.get("납부기한", "미정"))
        status = r.get("status", r.get("상태", ""))
        lines.append(
            f"  - {tax_type}: {format_krw(amount)} (기한: {due}) {status}"
        )

    return "\n".join(lines)


def _answer_cash(data: list, balance: dict, question: str) -> str:
    """현금 관련 질문에 대한 답변을 생성."""
    lines = ["[현금/자금 현황]"]

    if balance:
        cash = (
            balance.get("assets", {})
            .get("유동자산", {})
            .get("현금_보통예금", 0)
        )
        lines.append(f"  현재 잔고: {format_krw(cash)}")

    if data:
        for r in data[:5]:
            desc = r.get("description", r.get("설명", ""))
            bal = r.get("balance", r.get("잔액", 0))
            lines.append(f"  - {desc}: {format_krw(bal)}")
    elif not balance:
        lines.append("  현금 데이터가 없습니다. cash.py를 먼저 실행해 주세요.")

    return "\n".join(lines)


def _answer_asset(data: list, question: str) -> str:
    """자산 관련 질문에 대한 답변을 생성."""
    if not data:
        return "[자산] 자산 데이터가 없습니다. asset.py를 먼저 실행해 주세요."

    lines = ["[자산/설비 현황]"]
    total_book = 0

    for r in data[:10]:
        name = r.get("name", r.get("자산명", "설비"))
        acq = r.get("acquisition_cost", r.get("취득가", 0))
        bv = r.get("book_value", r.get("장부가", 0))
        dep = r.get("monthly_depreciation", r.get("월감가상각비", 0))

        try:
            total_book += int(float(bv))
        except (ValueError, TypeError):
            pass

        lines.append(
            f"  - {name}: 취득가 {format_krw(acq)}, "
            f"장부가 {format_krw(bv)}, 월상각 {format_krw(dep)}"
        )

    lines.append(f"\n  설비 장부가 합계: {format_krw(total_book)}")
    return "\n".join(lines)


# ── 5. 비상 모드 ──────────────────────────────────────────────

def emergency_mode():
    """비상 모드: RED 등급 알림만 즉시 표시."""
    print()
    print_separator("!", 50)
    print("  [EMERGENCY] 비상 알림 모드")
    print_separator("!", 50)
    print()

    alerts = load_all_alerts()
    if not alerts:
        print("  등록된 알림이 없습니다.")
        print("  outputs/alerts/ 디렉토리를 확인하세요.")
        return

    prioritized = prioritize_alerts(alerts)
    red_alerts = prioritized["red"]

    if not red_alerts:
        print("  [RED] 즉시 조치가 필요한 항목이 없습니다.")
        print(
            f"  (YELLOW {len(prioritized['yellow'])}건, "
            f"GREEN {len(prioritized['green'])}건 존재)"
        )
        return

    print(f"  [RED] 즉시 조치 필요: {len(red_alerts)}건\n")

    for i, alert in enumerate(red_alerts, 1):
        print(f"  {i}. {alert['message']}")
        if alert["d_day"] is not None:
            print(f"     D-day: {alert['d_day']}일")
        if alert["margin"] is not None:
            print(f"     마진율: {alert['margin']:.1f}%")
        print()

    print_separator("!", 50)
    print(f"  총 {len(red_alerts)}건의 즉시 조치 항목이 있습니다.")
    print_separator("!", 50)


# ── CLI ───────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="SOL-CFO: 스토리팜 최고재무책임자 에이전트",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--monthly-close",
        action="store_true",
        help="월간 마감 프로세스 실행 (전체 에이전트 오케스트레이션)",
    )
    group.add_argument(
        "--daily",
        action="store_true",
        help="일일 점검 모드 (핵심 에이전트만 실행 + 브리핑)",
    )
    group.add_argument(
        "--ask",
        type=str,
        metavar="QUESTION",
        help='자연어 질문 (예: "이번달 괜찮아?", "카드론 언제 다 갚아?")',
    )
    group.add_argument(
        "--emergency",
        action="store_true",
        help="비상 모드: RED 등급 알림만 표시",
    )

    parser.add_argument(
        "--month",
        default=datetime.now().strftime("%Y-%m"),
        help="대상 월 (YYYY-MM 형식, 기본: 이번 달)",
    )

    args = parser.parse_args()

    # 월 형식 검증
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        print(f"[ERROR] 잘못된 월 형식: {args.month} (YYYY-MM 형식 필요)")
        sys.exit(1)

    print_separator("=", 55)
    print("  SOL-CFO: 스토리팜 최고재무책임자")
    print(
        f"  대상월: {args.month} | "
        f"실행시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    print_separator("=", 55)

    if args.monthly_close:
        run_monthly_close(args.month)
    elif args.daily:
        run_daily(args.month)
    elif args.ask:
        answer_question(args.ask, args.month)
    elif args.emergency:
        emergency_mode()

    print()
    print_separator("=", 55)
    print("  SOL-CFO 작업 완료")
    print_separator("=", 55)


if __name__ == "__main__":
    main()
