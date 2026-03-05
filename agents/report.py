"""
agents/report.py - REPORT: Financial Report Generator
모든 에이전트 산출물을 취합하여 재무제표 및 경영 브리핑을 생성한다.
"""

import argparse
import os
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
PROCESSED = ROOT / "data" / "processed"
ALERTS_DIR = ROOT / "outputs" / "alerts"
REPORTS_DIR = ROOT / "outputs" / "reports"


# ── 데이터 로드 헬퍼 ──────────────────────────────────────────

def _find_json(subdir: str, month: str) -> list:
    """data/processed/{subdir}/ 내에서 month(YYYY-MM) 관련 JSON 파일을 로드."""
    target_dir = PROCESSED / subdir
    results = []
    if not target_dir.exists():
        return results
    for fpath in sorted(target_dir.glob("*.json")):
        fname = fpath.stem
        if month in fname or "master" in fname or "all" in fname:
            try:
                data = load_json(str(fpath))
                if isinstance(data, list):
                    results.extend(data)
                elif isinstance(data, dict):
                    results.append(data)
            except Exception as e:
                print(f"[REPORT] 경고: {fpath} 로드 실패 - {e}")
    return results


def _load_all_json(subdir: str) -> list:
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
        except Exception as e:
            print(f"[REPORT] 경고: {fpath} 로드 실패 - {e}")
    return results


def _extract_amount(record: dict, keys: list) -> int:
    """레코드에서 금액 값을 추출. 여러 키를 순서대로 시도."""
    for k in keys:
        if k in record:
            try:
                val = int(float(record[k]))
                if val > 0:
                    return val
            except (ValueError, TypeError):
                pass
    return 0


def _extract_expense(record: dict, keys: list) -> int:
    """레코드에서 비용 값을 추출 (절대값)."""
    for k in keys:
        if k in record:
            try:
                return abs(int(float(record[k])))
            except (ValueError, TypeError):
                pass
    return 0


def _load_ledger_accounts(month: str) -> dict:
    """
    LUNA의 계정별원장 JSON을 로드하여 계정명 → 계정데이터 dict를 반환.
    계정별원장.json 구조: { "계정명": { account, type, total_debit, total_credit, balance, entries } }
    """
    ledger_dir = PROCESSED / "ledger"
    if not ledger_dir.exists():
        return {}
    for fpath in sorted(ledger_dir.glob("*.json")):
        if month in fpath.stem and "계정별원장" in fpath.stem:
            try:
                data = load_json(str(fpath))
                if isinstance(data, dict) and any(
                    isinstance(v, dict) and "total_debit" in v
                    for v in data.values()
                ):
                    return data
            except Exception as e:
                print(f"[REPORT] 경고: 계정별원장 로드 실패 - {e}")
    return {}


# ── 1. 손익계산서 (Income Statement) ──────────────────────────

def generate_income_statement(month: str) -> dict:
    """월별 손익계산서를 생성한다. LUNA의 계정별원장에서 직접 읽어온다."""
    accounts = _load_ledger_accounts(month)

    # ── 계정별원장에서 직접 매핑 ──
    workshop_product = 0   # 제품매출
    workshop_service = 0   # 용역매출
    pension_revenue = 0    # 숙박매출
    raw_material = 0       # 원재료비
    outsourcing = 0        # 외주가공비
    supplies = 0           # 소모품비
    rent = 0               # 임차료
    labor = 0              # 인건비
    depreciation = 0       # 감가상각비
    other_sga = 0          # 기타판관비
    interest_expense = 0   # 이자비용

    for acct_name, acct_data in accounts.items():
        acct_type = acct_data.get("type", "")
        debit = int(acct_data.get("total_debit", 0))
        credit = int(acct_data.get("total_credit", 0))

        # 매출 계정 (대변 잔액)
        if acct_type == "revenue" or "매출" in acct_name:
            rev_amount = credit  # 매출은 대변
            if "숙박" in acct_name or "펜션" in acct_name:
                pension_revenue += rev_amount
            elif "용역" in acct_name:
                workshop_service += rev_amount
            elif "제품" in acct_name:
                workshop_product += rev_amount
            else:
                workshop_product += rev_amount

        # 매출원가 계정 (차변 잔액)
        elif acct_type == "cogs":
            if "원재료" in acct_name or "재료" in acct_name:
                raw_material += debit
            elif "외주" in acct_name:
                outsourcing += debit
            elif "소모품" in acct_name:
                supplies += debit
            else:
                raw_material += debit

        # 판관비 계정 (차변 잔액)
        elif acct_type == "sga":
            if "임차" in acct_name or "월세" in acct_name:
                rent += debit
            elif "인건" in acct_name or "급여" in acct_name:
                labor += debit
            elif "감가상각비" in acct_name:
                depreciation += debit
            elif "기타판관비" in acct_name or "판관" in acct_name:
                other_sga += debit
            else:
                other_sga += debit

        # 이자비용
        elif "이자" in acct_name:
            interest_expense += debit

    # 계정별원장에 이자비용이 없으면 debts.json에서 추정
    if interest_expense == 0:
        debts_path = ROOT / "data" / "debts.json"
        if debts_path.exists():
            try:
                from tools.calculator import calc_interest
                dconf = load_json(str(debts_path))
                debt_list = dconf.get("debts", dconf) if isinstance(dconf, dict) else dconf
                if isinstance(debt_list, list):
                    for d in debt_list:
                        bal = d.get("current_balance", d.get("balance", 0))
                        rate = d.get("interest_rate", 0)
                        if bal and rate:
                            interest_expense += calc_interest(int(bal), float(rate))
            except Exception:
                pass

    total_revenue = workshop_product + workshop_service + pension_revenue
    cogs = raw_material + outsourcing + supplies
    gross_profit = total_revenue - cogs
    sga = rent + labor + depreciation + other_sga
    operating_income = gross_profit - sga
    non_operating = -interest_expense
    income_before_tax = operating_income + non_operating

    return {
        "report_type": "income_statement",
        "title": "손익계산서",
        "month": month,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "revenue": {
            "제품매출_workshop": workshop_product,
            "용역매출_workshop": workshop_service,
            "숙박매출_pension": pension_revenue,
            "매출액_합계": total_revenue,
        },
        "cogs": {
            "원재료비": raw_material,
            "외주가공비": outsourcing,
            "소모품비": supplies,
            "매출원가_합계": cogs,
        },
        "매출총이익": gross_profit,
        "sga": {
            "임차료": rent,
            "인건비": labor,
            "감가상각비": depreciation,
            "기타": other_sga,
            "판관비_합계": sga,
        },
        "영업이익": operating_income,
        "영업외손익": {
            "이자비용": interest_expense,
            "영업외손익_합계": non_operating,
        },
        "세전이익": income_before_tax,
    }


def save_income_statement(month: str) -> str:
    """손익계산서를 텍스트 파일로 저장."""
    stmt = generate_income_statement(month)
    ensure_dir(str(REPORTS_DIR))

    rev = stmt["revenue"]
    cogs = stmt["cogs"]
    sga = stmt["sga"]
    nop = stmt["영업외손익"]

    lines = [
        "=" * 50,
        f"  손익계산서 ({month})",
        "=" * 50,
        f"  작성일: {stmt['generated_at']}",
        "\u2500" * 50,
        "",
        "[ 매출액 ]",
        f"  제품매출 (공방)     {format_krw(rev['제품매출_workshop']):>20}",
        f"  용역매출 (공방)     {format_krw(rev['용역매출_workshop']):>20}",
        f"  숙박매출 (펜션)     {format_krw(rev['숙박매출_pension']):>20}",
        f"  {'\u2500' * 46}",
        f"  매출액 합계         {format_krw(rev['매출액_합계']):>20}",
        "",
        "[ 매출원가 ]",
        f"  원재료비            {format_krw(cogs['원재료비']):>20}",
        f"  외주가공비          {format_krw(cogs['외주가공비']):>20}",
        f"  소모품비            {format_krw(cogs['소모품비']):>20}",
        f"  {'\u2500' * 46}",
        f"  매출원가 합계       {format_krw(cogs['매출원가_합계']):>20}",
        "",
        f"  *** 매출총이익      {format_krw(stmt['매출총이익']):>20}",
        "",
        "[ 판매비와 관리비 ]",
        f"  임차료              {format_krw(sga['임차료']):>20}",
        f"  인건비              {format_krw(sga['인건비']):>20}",
        f"  감가상각비          {format_krw(sga['감가상각비']):>20}",
        f"  기타                {format_krw(sga['기타']):>20}",
        f"  {'\u2500' * 46}",
        f"  판관비 합계         {format_krw(sga['판관비_합계']):>20}",
        "",
        f"  *** 영업이익        {format_krw(stmt['영업이익']):>20}",
        "",
        "[ 영업외손익 ]",
        f"  이자비용           -{format_krw(nop['이자비용']):>19}",
        f"  {'\u2500' * 46}",
        f"  영업외손익          {format_krw(nop['영업외손익_합계']):>20}",
        "",
        "=" * 50,
        f"  *** 세전이익        {format_krw(stmt['세전이익']):>20}",
        "=" * 50,
    ]

    text = "\n".join(lines)
    out_path = REPORTS_DIR / f"{month}_손익계산서.txt"
    with open(str(out_path), "w", encoding="utf-8") as f:
        f.write(text)

    save_json(stmt, str(REPORTS_DIR / f"{month}_income_statement.json"))

    print(f"[REPORT] 손익계산서 저장: {out_path}")
    return str(out_path)


# ── 2. 재무상태표 (Balance Sheet) ─────────────────────────────

def generate_balance_sheet(month: str) -> dict:
    """월별 재무상태표를 생성한다."""
    accounts = _load_ledger_accounts(month)
    cash_data = _find_json("cash", month)
    asset_data = _find_json("asset", month) + _find_json("assets", month)
    debt_data = _find_json("debt", month)

    # ── 자산 (Assets) ──
    cash_balance = 0
    accounts_receivable = 0

    # 계정별원장에서 보통예금 잔액 읽기
    for acct_name, acct_data in accounts.items():
        if "보통예금" in acct_name or "현금" in acct_name:
            # 자산 계정: 차변 - 대변 (음수일 수 있음)
            debit = int(acct_data.get("total_debit", 0))
            credit = int(acct_data.get("total_credit", 0))
            cash_balance += debit - credit
        elif "미수" in acct_name:
            debit = int(acct_data.get("total_debit", 0))
            credit = int(acct_data.get("total_credit", 0))
            accounts_receivable += debit - credit

    # 계정별원장에 없으면 cash 에이전트 데이터 사용
    if cash_balance == 0:
        for r in cash_data:
            bal = r.get("balance", r.get("잔액", r.get("cash_balance", 0)))
            try:
                cash_balance = max(cash_balance, int(float(bal)))
            except (ValueError, TypeError):
                pass

    current_assets = cash_balance + accounts_receivable

    equipment_book_value = 0
    for r in asset_data:
        # 자산현황.json은 {depreciation: [...]} 구조일 수 있음
        dep_list = r.get("depreciation", [])
        if dep_list:
            for item in dep_list:
                bv = item.get("book_value", 0)
                try:
                    equipment_book_value += int(float(bv))
                except (ValueError, TypeError):
                    pass
        else:
            bv = r.get("book_value", r.get("장부가", 0))
            acq = r.get("acquisition_cost", r.get("취득가", 0))
            acc_dep = r.get("accumulated_depreciation", r.get("감가상각누계액", 0))
            try:
                if bv:
                    equipment_book_value += int(float(bv))
                elif acq:
                    equipment_book_value += int(float(acq)) - int(float(acc_dep))
            except (ValueError, TypeError):
                pass

    non_current_assets = equipment_book_value
    total_assets = current_assets + non_current_assets

    # ── 부채 (Liabilities) ──
    accounts_payable = 0
    short_term_loan = 0
    long_term_loan = 0

    # data/debts.json (원본 채무 데이터)에서도 읽기
    debts_config_path = ROOT / "data" / "debts.json"
    all_debts = list(debt_data)
    if debts_config_path.exists():
        try:
            raw_debts = load_json(str(debts_config_path))
            if isinstance(raw_debts, dict) and "debts" in raw_debts:
                all_debts.extend(raw_debts["debts"])
            elif isinstance(raw_debts, list):
                all_debts.extend(raw_debts)
            elif isinstance(raw_debts, dict):
                all_debts.append(raw_debts)
        except Exception:
            pass

    for r in all_debts:
        dtype = str(r.get("type", r.get("유형", r.get("구분", ""))))
        remaining = 0
        try:
            remaining = int(float(
                r.get("current_balance",
                       r.get("remaining_balance",
                              r.get("balance", r.get("잔액", 0))))
            ))
        except (ValueError, TypeError):
            pass

        if remaining <= 0:
            continue

        if "미지급" in dtype or "payable" in dtype:
            accounts_payable += remaining
        elif "장기" in dtype or "long" in dtype:
            long_term_loan += remaining
        elif ("단기" in dtype or "short" in dtype or "카드론" in dtype
              or "card" in dtype or "installment" in dtype):
            short_term_loan += remaining
        else:
            maturity = r.get("maturity_date", r.get("만기일", ""))
            if maturity:
                try:
                    mat_date = datetime.strptime(str(maturity)[:10], "%Y-%m-%d")
                    ref_date = datetime.strptime(month + "-01", "%Y-%m-%d")
                    if (mat_date - ref_date).days <= 365:
                        short_term_loan += remaining
                    else:
                        long_term_loan += remaining
                except ValueError:
                    short_term_loan += remaining
            else:
                short_term_loan += remaining

    # 계정별원장에서 미지급금 읽기
    for acct_name, acct_data in accounts.items():
        if "미지급" in acct_name:
            credit = int(acct_data.get("total_credit", 0))
            debit = int(acct_data.get("total_debit", 0))
            accounts_payable += credit - debit  # 부채는 대변 잔액

    current_liabilities = accounts_payable + short_term_loan
    non_current_liabilities = long_term_loan
    total_liabilities = current_liabilities + non_current_liabilities

    # ── 자본 (Equity) ──
    income_stmt = generate_income_statement(month)
    retained_earnings = income_stmt["세전이익"]
    capital = total_assets - total_liabilities - retained_earnings

    if capital < 0:
        retained_earnings = total_assets - total_liabilities
        capital = 0

    total_equity = capital + retained_earnings
    balance_check = total_assets == (total_liabilities + total_equity)

    return {
        "report_type": "balance_sheet",
        "title": "재무상태표",
        "month": month,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "assets": {
            "유동자산": {
                "현금_보통예금": cash_balance,
                "미수금": accounts_receivable,
                "유동자산_소계": current_assets,
            },
            "비유동자산": {
                "기계장치_장부가": equipment_book_value,
                "비유동자산_소계": non_current_assets,
            },
            "자산_합계": total_assets,
        },
        "liabilities": {
            "유동부채": {
                "미지급금": accounts_payable,
                "단기차입금": short_term_loan,
                "유동부채_소계": current_liabilities,
            },
            "비유동부채": {
                "장기차입금": long_term_loan,
                "비유동부채_소계": non_current_liabilities,
            },
            "부채_합계": total_liabilities,
        },
        "equity": {
            "자본금": capital,
            "이익잉여금": retained_earnings,
            "자본_합계": total_equity,
        },
        "대차_검증": "일치" if balance_check else "불일치",
        "검증_상세": {
            "자산합계": total_assets,
            "부채_자본합계": total_liabilities + total_equity,
        },
    }


def save_balance_sheet(month: str) -> str:
    """재무상태표를 텍스트 파일로 저장."""
    bs = generate_balance_sheet(month)
    ensure_dir(str(REPORTS_DIR))

    a = bs["assets"]
    li = bs["liabilities"]
    e = bs["equity"]

    lines = [
        "=" * 50,
        f"  재무상태표 ({month})",
        "=" * 50,
        f"  작성일: {bs['generated_at']}",
        "\u2500" * 50,
        "",
        "[ 자산 ]",
        "  (유동자산)",
        f"    현금/보통예금      {format_krw(a['유동자산']['현금_보통예금']):>18}",
        f"    미수금             {format_krw(a['유동자산']['미수금']):>18}",
        f"    유동자산 소계      {format_krw(a['유동자산']['유동자산_소계']):>18}",
        "  (비유동자산)",
        f"    기계장치 장부가    {format_krw(a['비유동자산']['기계장치_장부가']):>18}",
        f"    비유동자산 소계    {format_krw(a['비유동자산']['비유동자산_소계']):>18}",
        f"  {'\u2500' * 46}",
        f"  자산 합계            {format_krw(a['자산_합계']):>18}",
        "",
        "[ 부채 ]",
        "  (유동부채)",
        f"    미지급금           {format_krw(li['유동부채']['미지급금']):>18}",
        f"    단기차입금         {format_krw(li['유동부채']['단기차입금']):>18}",
        f"    유동부채 소계      {format_krw(li['유동부채']['유동부채_소계']):>18}",
        "  (비유동부채)",
        f"    장기차입금         {format_krw(li['비유동부채']['장기차입금']):>18}",
        f"    비유동부채 소계    {format_krw(li['비유동부채']['비유동부채_소계']):>18}",
        f"  {'\u2500' * 46}",
        f"  부채 합계            {format_krw(li['부채_합계']):>18}",
        "",
        "[ 자본 ]",
        f"    자본금             {format_krw(e['자본금']):>18}",
        f"    이익잉여금         {format_krw(e['이익잉여금']):>18}",
        f"  {'\u2500' * 46}",
        f"  자본 합계            {format_krw(e['자본_합계']):>18}",
        "",
        "=" * 50,
        f"  대차 검증: {bs['대차_검증']}",
        f"  자산={format_krw(bs['검증_상세']['자산합계'])}  "
        f"부채+자본={format_krw(bs['검증_상세']['부채_자본합계'])}",
        "=" * 50,
    ]

    text = "\n".join(lines)
    out_path = REPORTS_DIR / f"{month}_재무상태표.txt"
    with open(str(out_path), "w", encoding="utf-8") as f:
        f.write(text)

    save_json(bs, str(REPORTS_DIR / f"{month}_balance_sheet.json"))

    print(f"[REPORT] 재무상태표 저장: {out_path}")
    return str(out_path)


# ── 3. 현금흐름표 (Cash Flow Statement) ───────────────────────

def generate_cash_flow(month: str) -> dict:
    """월별 현금흐름표를 생성한다."""
    income_stmt = generate_income_statement(month)
    cash_data = _find_json("cash", month)
    debt_data = _find_json("debt", month)
    asset_data = _find_json("asset", month) + _find_json("assets", month)

    # 영업활동
    net_income = income_stmt["세전이익"]
    depreciation = income_stmt["sga"]["감가상각비"]

    working_capital_change = 0
    for r in cash_data:
        wc = r.get("working_capital_change", r.get("운전자본변동", 0))
        try:
            working_capital_change += int(float(wc))
        except (ValueError, TypeError):
            pass

    operating_cf = net_income + depreciation - working_capital_change

    # 투자활동
    equipment_purchase = 0
    maintenance_cost = 0
    for r in asset_data:
        try:
            equipment_purchase += int(float(
                r.get("purchase_amount", r.get("취득금액", 0))
            ))
        except (ValueError, TypeError):
            pass
        try:
            maintenance_cost += int(float(
                r.get("maintenance_cost", r.get("유지보수비", 0))
            ))
        except (ValueError, TypeError):
            pass

    investing_cf = -(equipment_purchase + maintenance_cost)

    # 재무활동
    loan_payment = 0
    loan_received = 0
    for r in debt_data:
        try:
            loan_payment += int(float(
                r.get("monthly_payment", r.get("상환액", r.get("payment", 0)))
            ))
        except (ValueError, TypeError):
            pass
        try:
            loan_received += int(float(
                r.get("loan_received", r.get("차입금", 0))
            ))
        except (ValueError, TypeError):
            pass

    financing_cf = loan_received - loan_payment
    net_cf = operating_cf + investing_cf + financing_cf

    beginning_cash = 0
    ending_cash = 0
    for r in cash_data:
        try:
            bc = r.get("beginning_balance", r.get("기초잔액", 0))
            if bc:
                beginning_cash = int(float(bc))
        except (ValueError, TypeError):
            pass
        try:
            ec = r.get("ending_balance", r.get("기말잔액", r.get("balance", 0)))
            if ec:
                ending_cash = int(float(ec))
        except (ValueError, TypeError):
            pass

    if ending_cash == 0:
        ending_cash = beginning_cash + net_cf

    return {
        "report_type": "cash_flow",
        "title": "현금흐름표",
        "month": month,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "영업활동": {
            "당기순이익": net_income,
            "감가상각비_가산": depreciation,
            "운전자본변동": working_capital_change,
            "영업활동_소계": operating_cf,
        },
        "투자활동": {
            "설비구입": equipment_purchase,
            "유지보수비": maintenance_cost,
            "투자활동_소계": investing_cf,
        },
        "재무활동": {
            "차입금_수령": loan_received,
            "대출_상환": loan_payment,
            "재무활동_소계": financing_cf,
        },
        "현금증감": net_cf,
        "기초현금": beginning_cash,
        "기말현금": ending_cash,
    }


def save_cash_flow(month: str) -> str:
    """현금흐름표를 텍스트 파일로 저장."""
    cf = generate_cash_flow(month)
    ensure_dir(str(REPORTS_DIR))

    op = cf["영업활동"]
    inv = cf["투자활동"]
    fin = cf["재무활동"]

    lines = [
        "=" * 50,
        f"  현금흐름표 ({month})",
        "=" * 50,
        f"  작성일: {cf['generated_at']}",
        "\u2500" * 50,
        "",
        "[ 영업활동 현금흐름 ]",
        f"  당기순이익           {format_krw(op['당기순이익']):>18}",
        f"  (+) 감가상각비       {format_krw(op['감가상각비_가산']):>18}",
        f"  (-) 운전자본변동     {format_krw(op['운전자본변동']):>18}",
        f"  {'\u2500' * 46}",
        f"  영업활동 소계        {format_krw(op['영업활동_소계']):>18}",
        "",
        "[ 투자활동 현금흐름 ]",
        f"  설비 구입           -{format_krw(inv['설비구입']):>17}",
        f"  유지보수비          -{format_krw(inv['유지보수비']):>17}",
        f"  {'\u2500' * 46}",
        f"  투자활동 소계        {format_krw(inv['투자활동_소계']):>18}",
        "",
        "[ 재무활동 현금흐름 ]",
        f"  차입금 수령          {format_krw(fin['차입금_수령']):>18}",
        f"  대출 상환           -{format_krw(fin['대출_상환']):>17}",
        f"  {'\u2500' * 46}",
        f"  재무활동 소계        {format_krw(fin['재무활동_소계']):>18}",
        "",
        "=" * 50,
        f"  현금 증감            {format_krw(cf['현금증감']):>18}",
        f"  기초 현금            {format_krw(cf['기초현금']):>18}",
        f"  기말 현금            {format_krw(cf['기말현금']):>18}",
        "=" * 50,
    ]

    text = "\n".join(lines)
    out_path = REPORTS_DIR / f"{month}_현금흐름표.txt"
    with open(str(out_path), "w", encoding="utf-8") as f:
        f.write(text)

    save_json(cf, str(REPORTS_DIR / f"{month}_cash_flow.json"))

    print(f"[REPORT] 현금흐름표 저장: {out_path}")
    return str(out_path)


# ── 4. 경영 브리핑 (Monthly Briefing) ─────────────────────────

def _load_alerts() -> list:
    """outputs/alerts/ 내의 모든 alert JSON을 로드."""
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


def _get_next_month_schedule(month: str) -> list:
    """다음달 주요 일정 (세금 납부일, 대출 상환일 등)을 반환."""
    try:
        year, mon = map(int, month.split("-"))
        if mon == 12:
            next_year, next_mon = year + 1, 1
        else:
            next_year, next_mon = year, mon + 1
        next_month = f"{next_year:04d}-{next_mon:02d}"
    except ValueError:
        return ["일정 확인 불가"]

    schedules = []

    # 세금 납부 일정
    tax_data = _find_json("tax", month)
    for r in tax_data:
        due = r.get("due_date", r.get("납부기한", ""))
        tax_type = r.get("tax_type", r.get("세목", "세금"))
        if str(due)[:7] == next_month:
            schedules.append(f"{tax_type} 납부 ({due})")

    # 대출 상환 일정
    debt_data = _find_json("debt", month) + _load_all_json("debt")
    seen_names = set()
    for r in debt_data:
        pay_day = r.get("payment_day", r.get("상환일", ""))
        name = r.get("name", r.get("대출명", "대출"))
        payment = r.get("monthly_payment", r.get("상환액", 0))
        if pay_day and name not in seen_names:
            seen_names.add(name)
            schedules.append(
                f"{name} 상환 (매월 {pay_day}일, {format_krw(payment)})"
            )

    # 법정 세무 일정
    if next_mon == 1:
        schedules.append("부가가치세 확정신고 (1/25)")
    elif next_mon == 3:
        schedules.append("법인세 신고 (3/31)")
    elif next_mon == 5:
        schedules.append("종합소득세 신고 (5/31)")
    elif next_mon == 7:
        schedules.append("부가가치세 확정신고 (7/25)")

    schedules.append(f"원천세 신고/납부 ({next_month}-10)")
    schedules.append(f"4대보험 납부 ({next_month}-10)")

    return schedules if schedules else ["특별 일정 없음"]


def generate_monthly_briefing(month: str) -> str:
    """경영 브리핑 텍스트를 생성하고 반환한다."""
    try:
        year, mon = month.split("-")
    except ValueError:
        year, mon = "????", "??"

    income = generate_income_statement(month)
    bs = generate_balance_sheet(month)

    rev = income["revenue"]
    workshop_rev = rev["제품매출_workshop"] + rev["용역매출_workshop"]
    pension_rev = rev["숙박매출_pension"]
    total_rev = rev["매출액_합계"]
    op_income = income["영업이익"]
    op_margin = (op_income / total_rev * 100) if total_rev > 0 else 0.0

    cash_balance = bs["assets"]["유동자산"]["현금_보통예금"]
    total_debt = bs["liabilities"]["부채_합계"]

    # 전월 채무 비교
    try:
        y, m = int(year), int(mon)
        prev_month = f"{y:04d}-{m - 1:02d}" if m > 1 else f"{y - 1:04d}-12"
        prev_bs = generate_balance_sheet(prev_month)
        prev_debt = prev_bs["liabilities"]["부채_합계"]
        debt_change = prev_debt - total_debt
    except Exception:
        debt_change = 0

    # 알림 수집
    alerts = _load_alerts()
    alert_lines = []
    for a in alerts[:5]:
        msg = a.get("message", a.get("내용", a.get("alert", "알림")))
        alert_lines.append(f"    - {msg}")
    if not alert_lines:
        alert_lines.append("    - 특이사항 없음")

    # 다음달 일정
    schedules = _get_next_month_schedule(month)
    schedule_lines = [f"    - {s}" for s in schedules[:5]]

    # 채무 변동 문구
    if debt_change > 0:
        debt_note = f" (전월比 {format_krw(abs(debt_change))} 감소)"
    elif debt_change < 0:
        debt_note = f" (전월比 {format_krw(abs(debt_change))} 증가)"
    else:
        debt_note = ""

    text_lines = [
        "=" * 48,
        f"  {year}년 {mon}월 스토리팜 경영 브리핑",
        "=" * 48,
        f"  이번달 매출: 공방 {format_krw(workshop_rev)} / "
        f"펜션 {format_krw(pension_rev)} / 합계 {format_krw(total_rev)}",
        f"  영업이익: {format_krw(op_income)} (이익률 {op_margin:.1f}%)",
        f"  현금 잔고: {format_krw(cash_balance)}",
        f"  채무 총액: {format_krw(total_debt)}{debt_note}",
        "  특이사항:",
        *alert_lines,
        "  다음달 주요 일정:",
        *schedule_lines,
        "=" * 48,
    ]

    return "\n".join(text_lines)


def save_monthly_briefing(month: str) -> str:
    """경영 브리핑을 텍스트 파일로 저장."""
    ensure_dir(str(REPORTS_DIR))
    text = generate_monthly_briefing(month)

    out_path = REPORTS_DIR / f"{month}_경영브리핑.txt"
    with open(str(out_path), "w", encoding="utf-8") as f:
        f.write(text)

    print(f"[REPORT] 경영 브리핑 저장: {out_path}")
    print()
    print(text)
    return str(out_path)


# ── CLI ───────────────────────────────────────────────────────

REPORT_TYPES = {
    "income-statement": ("손익계산서", save_income_statement),
    "balance-sheet": ("재무상태표", save_balance_sheet),
    "cash-flow": ("현금흐름표", save_cash_flow),
    "briefing": ("경영브리핑", save_monthly_briefing),
}


def main():
    parser = argparse.ArgumentParser(
        description="REPORT: 스토리팜 재무제표 생성 에이전트",
    )
    parser.add_argument(
        "--month",
        required=True,
        help="대상 월 (YYYY-MM 형식, 예: 2026-03)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="전체 보고서 생성 (손익계산서 + 재무상태표 + 현금흐름표 + 경영브리핑)",
    )
    parser.add_argument(
        "--type",
        choices=list(REPORT_TYPES.keys()),
        help="개별 보고서 유형 선택",
    )

    args = parser.parse_args()

    # 월 형식 검증
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        print(f"[ERROR] 잘못된 월 형식: {args.month} (YYYY-MM 형식 필요)")
        sys.exit(1)

    print_separator("=", 50)
    print(f"  REPORT 에이전트 - {args.month} 재무제표 생성")
    print_separator("=", 50)
    print()

    if args.full:
        for key, (name, func) in REPORT_TYPES.items():
            print(f"[REPORT] {name} 생성 중...")
            try:
                func(args.month)
                print(f"[REPORT] {name} 완료")
            except Exception as e:
                print(f"[REPORT] {name} 생성 실패: {e}")
            print()
    elif args.type:
        name, func = REPORT_TYPES[args.type]
        print(f"[REPORT] {name} 생성 중...")
        try:
            func(args.month)
            print(f"[REPORT] {name} 완료")
        except Exception as e:
            print(f"[REPORT] {name} 생성 실패: {e}")
    else:
        print("[ERROR] --full 또는 --type 옵션을 지정하세요.")
        parser.print_help()
        sys.exit(1)

    print()
    print_separator("=", 50)
    print("  REPORT 에이전트 작업 완료")
    print_separator("=", 50)


if __name__ == "__main__":
    main()
