"""
storyfarm-finance / tools / parser.py
파일 파싱 유틸리티 - CSV, Excel 파일을 읽고 은행 포맷/파일 유형을 자동 감지
"""

import csv
import os
import sys
from typing import List, Dict, Optional

# openpyxl은 선택적 의존성
try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


def parse_csv(filepath: str, encoding: str = "utf-8") -> List[Dict]:
    """
    CSV 파일을 파싱하여 딕셔너리 리스트로 반환.
    첫 번째 행을 헤더로 사용한다.

    Args:
        filepath: CSV 파일 경로
        encoding: 파일 인코딩 (기본 utf-8, 실패 시 euc-kr 재시도)

    Returns:
        list of dicts - 각 행이 하나의 딕셔너리
    """
    filepath = os.path.abspath(filepath)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filepath}")

    # 한국 은행 내보내기 파일은 euc-kr인 경우가 많음
    for enc in [encoding, "euc-kr", "cp949"]:
        try:
            with open(filepath, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                rows = []
                for row in reader:
                    # 공백 키/값 정리
                    cleaned = {k.strip(): v.strip() if v else "" for k, v in row.items() if k}
                    rows.append(cleaned)
                return rows
        except (UnicodeDecodeError, UnicodeError):
            continue

    raise UnicodeDecodeError(f"지원되는 인코딩으로 파일을 읽을 수 없습니다: {filepath}")


def parse_excel(filepath: str, sheet_name: Optional[str] = None) -> List[Dict]:
    """
    Excel(.xlsx) 파일을 파싱하여 딕셔너리 리스트로 반환.
    openpyxl이 없으면 .csv로 폴백 시도.

    Args:
        filepath: Excel 파일 경로
        sheet_name: 시트 이름 (None이면 활성 시트)

    Returns:
        list of dicts - 각 행이 하나의 딕셔너리
    """
    filepath = os.path.abspath(filepath)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filepath}")

    # openpyxl 사용 가능한 경우
    if HAS_OPENPYXL:
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        if sheet_name:
            ws = wb[sheet_name]
        else:
            ws = wb.active

        rows_iter = ws.iter_rows(values_only=True)

        # 첫 행을 헤더로
        try:
            headers = [str(h).strip() if h is not None else f"col_{i}"
                       for i, h in enumerate(next(rows_iter))]
        except StopIteration:
            wb.close()
            return []

        result = []
        for row in rows_iter:
            if all(cell is None for cell in row):
                continue  # 빈 행 스킵
            row_dict = {}
            for i, cell in enumerate(row):
                key = headers[i] if i < len(headers) else f"col_{i}"
                value = cell if cell is not None else ""
                row_dict[key] = value
            result.append(row_dict)

        wb.close()
        return result

    # openpyxl 없으면 CSV 폴백 시도
    csv_path = filepath.rsplit(".", 1)[0] + ".csv"
    if os.path.exists(csv_path):
        print(f"[parser] openpyxl 미설치. CSV 폴백: {csv_path}")
        return parse_csv(csv_path)

    raise ImportError(
        "openpyxl이 설치되지 않았고, 대응하는 CSV 파일도 없습니다. "
        "pip install openpyxl 을 실행하세요."
    )


# ── 은행 포맷 감지용 헤더 패턴 ──────────────────────────────────

BANK_HEADER_PATTERNS = {
    "kookmin": [
        {"거래일시", "적요", "출금액", "입금액", "잔액"},
        {"거래일", "적요", "출금", "입금", "잔액"},
        {"거래일시", "내용", "출금액(원)", "입금액(원)", "잔액(원)"},
    ],
    "ibk": [
        {"거래일자", "거래시간", "적요", "출금금액", "입금금액", "거래후잔액"},
        {"거래일자", "적요", "출금", "입금", "잔액", "거래점"},
        {"거래일", "거래시간", "적요", "출금금액", "입금금액", "잔액"},
    ],
}


def detect_bank_format(headers: List[str]) -> str:
    """
    CSV/Excel 헤더 리스트를 보고 은행 포맷을 감지.

    Args:
        headers: 컬럼 헤더 문자열 리스트

    Returns:
        "kookmin" | "ibk" | "unknown"
    """
    header_set = {h.strip() for h in headers}

    for bank, patterns in BANK_HEADER_PATTERNS.items():
        for pattern in patterns:
            if pattern.issubset(header_set):
                return bank

    # 부분 매칭 시도 (헤더에 은행 키워드 포함 여부)
    header_joined = " ".join(headers).lower()
    if "국민" in header_joined or "kb" in header_joined:
        return "kookmin"
    if "기업" in header_joined or "ibk" in header_joined:
        return "ibk"

    return "unknown"


# ── 파일 유형 감지 ────────────────────────────────────────────

FILE_TYPE_KEYWORDS = {
    "tax_invoice": [
        "세금계산서", "공급가액", "세액", "공급받는자", "등록번호",
        "tax_invoice", "공급자", "발급일",
    ],
    "bank_statement": [
        "거래일시", "거래일자", "출금액", "입금액", "잔액",
        "출금금액", "입금금액", "거래후잔액", "적요",
    ],
    "order": [
        "주문번호", "주문일", "상품명", "수량", "단가",
        "order_id", "order_date", "product", "qty",
        "주문자", "배송지",
    ],
    "reservation": [
        "예약번호", "체크인", "체크아웃", "객실", "투숙객",
        "reservation", "check_in", "check_out", "guest",
        "예약일", "숙박일",
    ],
}


def detect_file_type(filepath: str) -> str:
    """
    파일 내용을 읽어 문서 유형을 감지.
    헤더 및 초반 내용의 키워드 매칭으로 판단한다.

    Args:
        filepath: 파일 경로 (CSV 또는 Excel)

    Returns:
        "tax_invoice" | "bank_statement" | "order" | "reservation" | "unknown"
    """
    filepath = os.path.abspath(filepath)
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {filepath}")

    ext = os.path.splitext(filepath)[1].lower()

    # 파일 내용 샘플 수집
    sample_text = ""
    headers = []

    try:
        if ext in (".xlsx", ".xls"):
            rows = parse_excel(filepath)
        else:
            rows = parse_csv(filepath)

        if rows:
            headers = list(rows[0].keys())
            # 처음 5행까지 텍스트 수집
            for row in rows[:5]:
                sample_text += " ".join(str(v) for v in row.values()) + " "
            sample_text += " ".join(headers)
    except Exception:
        # 바이너리가 아닌 텍스트 파일로 직접 읽기 시도
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                sample_text = f.read(2000)
        except Exception:
            return "unknown"

    sample_lower = sample_text.lower()

    # 키워드 매칭 점수 계산
    scores = {}
    for file_type, keywords in FILE_TYPE_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw.lower() in sample_lower)
        if score > 0:
            scores[file_type] = score

    if not scores:
        return "unknown"

    return max(scores, key=scores.get)


# ── 테스트 블록 ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=== parser.py 단위 테스트 ===\n")

    # detect_bank_format 테스트
    kb_headers = ["거래일시", "적요", "출금액", "입금액", "잔액", "메모"]
    ibk_headers = ["거래일자", "거래시간", "적요", "출금금액", "입금금액", "거래후잔액"]
    unknown_headers = ["날짜", "항목", "금액"]

    assert detect_bank_format(kb_headers) == "kookmin", "국민은행 감지 실패"
    assert detect_bank_format(ibk_headers) == "ibk", "IBK 감지 실패"
    assert detect_bank_format(unknown_headers) == "unknown", "unknown 감지 실패"
    print("[PASS] detect_bank_format: kookmin, ibk, unknown 모두 정상")

    # detect_file_type - 헤더 기반 키워드 매칭 간접 테스트
    # (실제 파일 없이 FILE_TYPE_KEYWORDS 구조 확인)
    assert "tax_invoice" in FILE_TYPE_KEYWORDS
    assert "bank_statement" in FILE_TYPE_KEYWORDS
    assert "order" in FILE_TYPE_KEYWORDS
    assert "reservation" in FILE_TYPE_KEYWORDS
    print("[PASS] FILE_TYPE_KEYWORDS 구조 정상")

    print(f"\n[INFO] openpyxl 설치 여부: {HAS_OPENPYXL}")
    print("\n모든 테스트 통과!")
