"""
storyfarm-finance / tools / calculator.py
재무 계산 유틸리티 - 감가상각, 이자, 대출상환, 마진, BEP, 부가세
"""

import math
from typing import Dict, Tuple


def calc_depreciation_straight(
    cost: int,
    residual_rate: float,
    years: int,
    months_elapsed: int
) -> Dict[str, int]:
    """
    정액법 감가상각 계산.

    Args:
        cost: 취득원가 (원)
        residual_rate: 잔존가치율 (예: 0.1 = 10%)
        years: 내용연수 (년)
        months_elapsed: 경과 개월 수

    Returns:
        dict with keys:
            monthly_amount: 월 감가상각비
            accumulated: 감가상각누계액
            book_value: 장부가액
    """
    residual_value = int(cost * residual_rate)
    depreciable_amount = cost - residual_value
    total_months = years * 12

    monthly_amount = int(depreciable_amount / total_months)

    # 경과 개월이 내용연수 초과 시 전액 상각
    effective_months = min(months_elapsed, total_months)
    accumulated = monthly_amount * effective_months

    # 마지막 달 단수 차이 보정
    if effective_months == total_months:
        accumulated = depreciable_amount

    book_value = cost - accumulated

    return {
        "monthly_amount": monthly_amount,
        "accumulated": accumulated,
        "book_value": book_value,
    }


def calc_depreciation_declining(
    cost: int,
    residual_rate: float,
    years: int,
    months_elapsed: int
) -> Dict[str, int]:
    """
    정률법 감가상각 계산.
    한국 세법 정률법 상각률 = 1 - (잔존가치율)^(1/내용연수)

    Args:
        cost: 취득원가 (원)
        residual_rate: 잔존가치율 (예: 0.1 = 10%)
        years: 내용연수 (년)
        months_elapsed: 경과 개월 수

    Returns:
        dict with keys:
            monthly_amount: 해당 월의 감가상각비
            accumulated: 감가상각누계액
            book_value: 장부가액
    """
    residual_value = int(cost * residual_rate)

    # 정률법 상각률 계산
    if residual_rate > 0:
        declining_rate = 1 - (residual_rate ** (1 / years))
    else:
        # 잔존가치 0이면 정액법과 동일하게 처리
        return calc_depreciation_straight(cost, 0, years, months_elapsed)

    total_months = years * 12
    effective_months = min(months_elapsed, total_months)

    # 월별로 순차 계산 (정률법은 매 기간 장부가액 기준)
    book_value = cost
    accumulated = 0
    monthly_amount = 0

    for m in range(1, effective_months + 1):
        # 연간 상각액을 12로 나눠 월할
        annual_depreciation = int(book_value * declining_rate)
        month_dep = int(annual_depreciation / 12)

        # 장부가액이 잔존가치 이하로 내려가지 않도록
        if book_value - month_dep < residual_value:
            month_dep = book_value - residual_value
            if month_dep < 0:
                month_dep = 0

        accumulated += month_dep
        book_value = cost - accumulated
        monthly_amount = month_dep  # 마지막 달의 상각비

    return {
        "monthly_amount": monthly_amount,
        "accumulated": accumulated,
        "book_value": book_value,
    }


def calc_interest(balance: int, annual_rate: float) -> int:
    """
    월 이자 계산 (단리).

    Args:
        balance: 잔액 (원)
        annual_rate: 연이율 (예: 0.195 = 19.5%)

    Returns:
        월 이자 (원, 정수)
    """
    return int(balance * annual_rate / 12)


def calc_loan_payoff(
    balance: int,
    rate: float,
    monthly_payment: int,
    extra: int = 0
) -> Dict[str, object]:
    """
    대출 상환 시뮬레이션 - 잔여 개월 수와 총 이자 계산.

    Args:
        balance: 현재 잔액 (원)
        rate: 연이율 (예: 0.12 = 12%)
        monthly_payment: 월 상환액 (원)
        extra: 추가 상환액 (원, 기본 0)

    Returns:
        dict with keys:
            months_remaining: 잔여 개월
            total_interest: 총 이자 합계
    """
    total_payment = monthly_payment + extra
    monthly_rate = rate / 12

    if total_payment <= 0:
        return {"months_remaining": float("inf"), "total_interest": float("inf")}

    remaining = balance
    months = 0
    total_interest = 0
    max_months = 600  # 50년 안전장치

    while remaining > 0 and months < max_months:
        interest = int(remaining * monthly_rate)
        total_interest += interest

        principal = total_payment - interest
        if principal <= 0:
            # 이자도 못 갚는 경우
            return {"months_remaining": float("inf"), "total_interest": float("inf")}

        remaining -= principal
        months += 1

        if remaining < 0:
            remaining = 0

    return {
        "months_remaining": months,
        "total_interest": total_interest,
    }


def calc_margin(revenue: int, cost: int) -> float:
    """
    마진율 계산.

    Args:
        revenue: 매출액 (원)
        cost: 원가 (원)

    Returns:
        마진율 (0.0 ~ 1.0). 매출 0이면 0.0 반환.
    """
    if revenue == 0:
        return 0.0
    return (revenue - cost) / revenue


def calc_bep(fixed_cost: int, variable_cost_ratio: float) -> int:
    """
    손익분기점(BEP) 매출액 계산.

    Args:
        fixed_cost: 고정비 (원)
        variable_cost_ratio: 변동비율 (예: 0.6 = 매출의 60%가 변동비)

    Returns:
        BEP 매출액 (원). 변동비율이 1.0 이상이면 inf 반환.
    """
    if variable_cost_ratio >= 1.0:
        return float("inf")
    return int(fixed_cost / (1 - variable_cost_ratio))


def calc_vat(supply_amount: int, tax_type: str = "taxable") -> int:
    """
    부가가치세 계산.

    Args:
        supply_amount: 공급가액 (원)
        tax_type: "taxable" (과세) | "exempt" (면세) | "zero" (영세율)

    Returns:
        부가세 금액 (원)
    """
    if tax_type == "taxable":
        return int(supply_amount * 0.1)
    elif tax_type in ("exempt", "zero"):
        return 0
    else:
        raise ValueError(f"알 수 없는 세금 유형: {tax_type}. 'taxable', 'exempt', 'zero' 중 선택")


def format_krw(amount) -> str:
    """
    금액을 한국 원화 형식으로 포맷.

    Args:
        amount: 금액 (int 또는 float)

    Returns:
        포맷된 문자열 (예: "₩1,234,567" 또는 "-₩500,000")
    """
    amount = int(amount)
    if amount < 0:
        return f"-\u20a9{abs(amount):,}"
    return f"\u20a9{amount:,}"


# ── 테스트 블록 ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=== calculator.py 단위 테스트 ===\n")

    # 1. 정액법 감가상각 (CNC 머신 - assets.json 기준)
    result = calc_depreciation_straight(
        cost=30_000_000, residual_rate=0.1, years=5, months_elapsed=33
    )
    print(f"[정액법] CNC 30,000,000원 / 5년 / 33개월 경과:")
    print(f"  월 상각비:       {format_krw(result['monthly_amount'])}")
    print(f"  감가상각누계액:  {format_krw(result['accumulated'])}")
    print(f"  장부가액:        {format_krw(result['book_value'])}")
    assert result["monthly_amount"] == 450000, f"월 상각비 오류: {result['monthly_amount']}"
    assert result["accumulated"] == 14850000, f"누계액 오류: {result['accumulated']}"
    assert result["book_value"] == 15150000, f"장부가 오류: {result['book_value']}"
    print("  [PASS]\n")

    # 2. 정률법 감가상각
    result2 = calc_depreciation_declining(
        cost=30_000_000, residual_rate=0.1, years=5, months_elapsed=12
    )
    print(f"[정률법] 30,000,000원 / 5년 / 12개월 경과:")
    print(f"  현재 월 상각비:  {format_krw(result2['monthly_amount'])}")
    print(f"  감가상각누계액:  {format_krw(result2['accumulated'])}")
    print(f"  장부가액:        {format_krw(result2['book_value'])}")
    assert result2["book_value"] > 0, "장부가액이 0 이하"
    assert result2["accumulated"] > 0, "누계액이 0"
    print("  [PASS]\n")

    # 3. 월 이자 계산 (카드론 - debts.json 기준)
    interest = calc_interest(3_000_000, 0.195)
    print(f"[이자] 카드론 3,000,000원 @ 19.5%: {format_krw(interest)}/월")
    assert interest == 48750, f"이자 계산 오류: {interest}"
    print("  [PASS]\n")

    # 4. 대출 상환 시뮬레이션
    payoff = calc_loan_payoff(3_000_000, 0.195, 250_000)
    print(f"[상환] 카드론 월 250,000원 상환:")
    print(f"  잔여: {payoff['months_remaining']}개월")
    print(f"  총 이자: {format_krw(payoff['total_interest'])}")
    assert payoff["months_remaining"] > 0
    print("  [PASS]\n")

    # 5. 추가상환 효과 비교
    payoff_extra = calc_loan_payoff(3_000_000, 0.195, 250_000, extra=100_000)
    saved = payoff["total_interest"] - payoff_extra["total_interest"]
    print(f"[추가상환] 월 100,000원 추가 시:")
    print(f"  잔여: {payoff_extra['months_remaining']}개월 (단축 {payoff['months_remaining'] - payoff_extra['months_remaining']}개월)")
    print(f"  이자 절감: {format_krw(saved)}")
    assert payoff_extra["months_remaining"] < payoff["months_remaining"]
    print("  [PASS]\n")

    # 6. 마진율
    margin = calc_margin(1_000_000, 600_000)
    print(f"[마진] 매출 1,000,000 / 원가 600,000: {margin:.1%}")
    assert abs(margin - 0.4) < 0.001
    print("  [PASS]\n")

    # 7. BEP
    bep = calc_bep(5_000_000, 0.6)
    print(f"[BEP] 고정비 5,000,000 / 변동비율 60%: {format_krw(bep)}")
    assert bep == 12_500_000
    print("  [PASS]\n")

    # 8. 부가세
    vat = calc_vat(1_000_000)
    vat_exempt = calc_vat(1_000_000, "exempt")
    print(f"[VAT] 공급가 1,000,000 과세: {format_krw(vat)}, 면세: {format_krw(vat_exempt)}")
    assert vat == 100_000
    assert vat_exempt == 0
    print("  [PASS]\n")

    # 9. format_krw
    assert format_krw(1234567) == "\u20a91,234,567"
    assert format_krw(-500000) == "-\u20a9500,000"
    assert format_krw(0) == "\u20a90"
    print("[PASS] format_krw 포맷 정상\n")

    print("모든 테스트 통과!")
