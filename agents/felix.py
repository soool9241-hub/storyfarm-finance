#!/usr/bin/env python3
"""
FELIX - Data Collector & Cleaner
=================================
data/raw/ 의 CSV·Excel 파일을 스캔하여 표준 JSON 트랜잭션으로 변환한다.

출력:
  data/processed/transactions/YYYY-MM_transactions.json
  outputs/alerts/felix_missing_YYYYMMDD.json  (누락 필드)

CLI:
  python agents/felix.py --input data/raw/ --month 2026-03
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tools.formatter import save_json, load_json, get_project_root, ensure_dir


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MATERIAL_KEYWORDS: dict[str, str] = {
    "AL6061": "AL6061",
    "AL5052": "AL5052",
    "SUS304": "SUS304",
    "SUS": "SUS304",
    "MDF": "MDF",
    "알루미늄6061": "AL6061",
    "알루미늄5052": "AL5052",
    "스테인리스": "SUS304",
}

CATEGORY_RULES: list[tuple[list[str], str]] = [
    (["재료", "소재", "알루미늄", "스테인", "MDF", "원자재", "철물"], "재료비"),
    (["급여", "인건", "일당", "노임", "아르바이트", "급료"], "인건비"),
    (["임대", "월세", "렌트", "관리비"], "임대료"),
    (["장비", "기계", "공구", "부품", "수리"], "장비"),
]

FILE_TYPE_PATTERNS: dict[str, list[str]] = {
    "tax_invoice": ["세금계산서", "tax_invoice", "taxinvoice", "계산서"],
    "bank_statement": ["입출금", "bank", "거래내역", "통장"],
    "cnc_order": ["주문", "order", "수주", "발주", "작업지시"],
    "pension_reservation": ["예약", "reservation", "숙박", "pension", "달팽이"],
}


# ---------------------------------------------------------------------------
# File Detection
# ---------------------------------------------------------------------------

def detect_file_type(filename: str, header_row: list[str] | None = None) -> str:
    """파일명과 헤더를 기반으로 파일 유형을 추정한다."""
    fn_lower = filename.lower()
    for ftype, keywords in FILE_TYPE_PATTERNS.items():
        for kw in keywords:
            if kw.lower() in fn_lower:
                return ftype

    # Fallback: 헤더 기반 추정
    if header_row:
        joined = " ".join(str(c).lower() for c in header_row)
        if "공급가액" in joined or "세액" in joined:
            return "tax_invoice"
        if "잔액" in joined or "balance" in joined:
            return "bank_statement"
        if "수량" in joined and ("단가" in joined or "unit" in joined):
            return "cnc_order"
        if "체크인" in joined or "checkin" in joined or "숙박" in joined:
            return "pension_reservation"

    return "unknown"


def detect_material(text: str) -> str | None:
    """텍스트 내 재료 키워드를 탐지한다."""
    upper = text.upper()
    for keyword, mat in MATERIAL_KEYWORDS.items():
        if keyword.upper() in upper:
            return mat
    return None


def classify_category(text: str) -> str:
    """거래 내역 텍스트로부터 비용 카테고리를 분류한다."""
    for keywords, cat in CATEGORY_RULES:
        for kw in keywords:
            if kw in text:
                return cat
    return "기타"


# ---------------------------------------------------------------------------
# Read files (CSV / Excel)
# ---------------------------------------------------------------------------

def read_csv_file(filepath: Path) -> list[dict[str, Any]]:
    """CSV 파일을 읽어 dict 리스트로 반환한다.
    인코딩은 utf-8-sig → cp949 순으로 시도한다."""
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            with open(filepath, "r", encoding=enc, newline="") as f:
                sample = f.read(4096)
            dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
            with open(filepath, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f, dialect=dialect)
                return list(reader)
        except (UnicodeDecodeError, csv.Error):
            continue

    # 최후 시도: 단순 comma
    with open(filepath, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def read_excel_file(filepath: Path) -> list[dict[str, Any]]:
    """Excel(xlsx/xls) 파일을 읽어 dict 리스트로 반환한다.
    openpyxl 이 없으면 빈 리스트를 반환한다."""
    try:
        import openpyxl
    except ImportError:
        print(f"  [WARN] openpyxl 미설치 → Excel 파일 건너뜀: {filepath.name}")
        return []

    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()
    if len(rows) < 2:
        return []
    headers = [str(h).strip() if h else f"col_{i}" for i, h in enumerate(rows[0])]
    result = []
    for row in rows[1:]:
        if all(v is None for v in row):
            continue
        result.append({headers[i]: row[i] for i in range(min(len(headers), len(row)))})
    return result


def read_data_file(filepath: Path) -> list[dict[str, Any]]:
    """파일 확장자에 따라 적절한 리더를 호출한다."""
    ext = filepath.suffix.lower()
    if ext == ".csv":
        return read_csv_file(filepath)
    elif ext in (".xlsx", ".xls"):
        return read_excel_file(filepath)
    return []


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_date(val: Any) -> str | None:
    """다양한 날짜 형식을 YYYY-MM-DD 문자열로 변환한다."""
    if val is None:
        return None
    if isinstance(val, (date, datetime)):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y%m%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_int(val: Any) -> int:
    """값을 정수로 변환한다. 실패 시 0."""
    if val is None:
        return 0
    s = str(val).replace(",", "").replace("₩", "").replace("원", "").strip()
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def find_column(row: dict, candidates: list[str]) -> Any:
    """row 딕셔너리에서 candidates 중 존재하는 첫 번째 키의 값을 반환한다."""
    for c in candidates:
        for key in row:
            if key and c.lower() in str(key).lower():
                return row[key]
    return None


def make_txn_id(d: str, seq: int) -> str:
    """TXN-YYYYMMDD-NNN 형식 ID를 생성한다."""
    ds = d.replace("-", "") if d else "00000000"
    return f"TXN-{ds}-{seq:03d}"


# ---------------------------------------------------------------------------
# Parsers per file type
# ---------------------------------------------------------------------------

def parse_tax_invoice(rows: list[dict], source_file: str) -> list[dict]:
    """세금계산서 파싱: 매출/매입 구분, 공급가액·세액 추출."""
    transactions: list[dict] = []
    for row in rows:
        raw_date = find_column(row, ["날짜", "일자", "작성일", "date"])
        supplier = find_column(row, ["공급자", "거래처", "상호", "supplier", "업체"])
        supply_amt = find_column(row, ["공급가액", "공급가", "supply", "amount", "금액"])
        tax_amt = find_column(row, ["세액", "tax", "부가세"])
        items = find_column(row, ["품목", "item", "내역", "품명"])
        biz_no = find_column(row, ["사업자번호", "사업자", "business_number"])
        direction = find_column(row, ["구분", "type", "유형", "매출매입"])

        d = parse_date(raw_date)
        supply = parse_int(supply_amt)
        tax = parse_int(tax_amt)
        total = supply + tax

        # 매출/매입 판정
        is_income = False
        dir_str = str(direction).strip() if direction else ""
        if "매출" in dir_str or "income" in dir_str.lower():
            is_income = True
        elif "매입" in dir_str or "expense" in dir_str.lower():
            is_income = False
        else:
            # 금액 부호로 추정
            is_income = supply >= 0

        txn = {
            "date": d,
            "type": "income" if is_income else "expense",
            "business_type": "workshop",
            "amount": abs(total),
            "tax_amount": abs(tax),
            "counterparty": str(supplier).strip() if supplier else None,
            "category": "매출" if is_income else "매입",
            "material": detect_material(str(items) if items else ""),
            "source_file": source_file,
            "raw_data": {k: str(v) for k, v in row.items()},
        }
        transactions.append(txn)
    return transactions


def parse_bank_statement(rows: list[dict], source_file: str) -> list[dict]:
    """입출금내역 파싱: 자동 카테고리 분류."""
    transactions: list[dict] = []
    for row in rows:
        raw_date = find_column(row, ["날짜", "일자", "거래일", "date"])
        counterparty = find_column(row, ["적요", "거래처", "상대", "내용", "메모", "counterparty", "description"])
        amount_val = find_column(row, ["금액", "거래금액", "amount"])
        deposit = find_column(row, ["입금", "입금액", "deposit", "credit"])
        withdraw = find_column(row, ["출금", "출금액", "withdraw", "debit"])
        balance = find_column(row, ["잔액", "잔고", "balance"])
        txn_type = find_column(row, ["구분", "type", "입출금구분"])

        d = parse_date(raw_date)
        cp = str(counterparty).strip() if counterparty else None
        cat_text = (cp or "") + " " + str(find_column(row, ["적요", "메모", "내용"]) or "")

        # 금액·유형 결정
        dep = parse_int(deposit)
        wit = parse_int(withdraw)
        amt = parse_int(amount_val)

        if dep > 0:
            is_income = True
            final_amount = dep
        elif wit > 0:
            is_income = False
            final_amount = wit
        elif amt != 0:
            type_str = str(txn_type).strip() if txn_type else ""
            if "입금" in type_str or "income" in type_str.lower() or amt > 0:
                is_income = True
                final_amount = abs(amt)
            else:
                is_income = False
                final_amount = abs(amt)
        else:
            is_income = False
            final_amount = 0

        # 펜션 vs 공방
        btype = "workshop"
        if cp and any(kw in cp for kw in ["달팽이", "펜션", "숙박", "에어비앤비", "여기어때", "야놀자"]):
            btype = "pension"

        txn = {
            "date": d,
            "type": "income" if is_income else "expense",
            "business_type": btype,
            "amount": final_amount,
            "tax_amount": 0,
            "counterparty": cp,
            "category": classify_category(cat_text),
            "material": detect_material(cat_text),
            "source_file": source_file,
            "raw_data": {k: str(v) for k, v in row.items()},
        }
        transactions.append(txn)
    return transactions


def parse_cnc_order(rows: list[dict], source_file: str) -> list[dict]:
    """CNC 수주·발주 파싱."""
    transactions: list[dict] = []
    for row in rows:
        raw_date = find_column(row, ["날짜", "일자", "주문일", "date", "발주일"])
        customer = find_column(row, ["고객", "거래처", "customer", "발주처", "업체"])
        item = find_column(row, ["품목", "item", "품명", "제품", "내역"])
        qty = find_column(row, ["수량", "quantity", "qty"])
        unit_price = find_column(row, ["단가", "unit_price", "price"])
        due = find_column(row, ["납기", "due", "납품일", "due_date"])
        material = find_column(row, ["소재", "재료", "material", "재질"])
        order_no = find_column(row, ["주문번호", "order_no", "order_number", "번호"])

        d = parse_date(raw_date)
        q = parse_int(qty)
        up = parse_int(unit_price)
        total = q * up if q and up else parse_int(find_column(row, ["금액", "합계", "total", "amount"]))

        mat_text = str(material or "") + " " + str(item or "")
        detected_mat = detect_material(mat_text)

        txn = {
            "date": d,
            "type": "income",
            "business_type": "workshop",
            "amount": abs(total),
            "tax_amount": 0,
            "counterparty": str(customer).strip() if customer else None,
            "category": "CNC가공",
            "material": detected_mat,
            "source_file": source_file,
            "raw_data": {k: str(v) for k, v in row.items()},
        }
        transactions.append(txn)
    return transactions


def parse_pension_reservation(rows: list[dict], source_file: str) -> list[dict]:
    """펜션 예약 파싱."""
    transactions: list[dict] = []
    for row in rows:
        raw_date = find_column(row, ["예약일", "날짜", "일자", "reservation_date", "date"])
        checkin = find_column(row, ["체크인", "checkin", "입실"])
        checkout = find_column(row, ["체크아웃", "checkout", "퇴실"])
        guests = find_column(row, ["인원", "guests", "guest_count"])
        amount = find_column(row, ["금액", "amount", "요금", "결제금액"])
        channel = find_column(row, ["채널", "channel", "예약경로", "플랫폼"])

        d = parse_date(checkin) or parse_date(raw_date)
        amt = parse_int(amount)

        txn = {
            "date": d,
            "type": "income",
            "business_type": "pension",
            "amount": abs(amt),
            "tax_amount": 0,
            "counterparty": str(channel).strip() if channel else "직접예약",
            "category": "숙박매출",
            "material": None,
            "source_file": source_file,
            "raw_data": {k: str(v) for k, v in row.items()},
        }
        transactions.append(txn)
    return transactions


PARSERS = {
    "tax_invoice": parse_tax_invoice,
    "bank_statement": parse_bank_statement,
    "cnc_order": parse_cnc_order,
    "pension_reservation": parse_pension_reservation,
}


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def scan_raw_files(raw_dir: Path) -> list[Path]:
    """data/raw/ 하위의 CSV, Excel 파일 경로 목록을 반환한다."""
    files: list[Path] = []
    if not raw_dir.exists():
        print(f"  [WARN] 입력 디렉터리가 존재하지 않습니다: {raw_dir}")
        return files
    for f in sorted(raw_dir.rglob("*")):
        if f.suffix.lower() in (".csv", ".xlsx", ".xls") and not f.name.startswith("~"):
            files.append(f)
    return files


def check_missing_fields(txn: dict) -> list[str]:
    """필수 필드 누락 여부를 점검하여 누락 필드명 리스트를 반환한다."""
    required = ["date", "type", "amount", "counterparty"]
    missing = []
    for field in required:
        val = txn.get(field)
        if val is None or (isinstance(val, str) and val.strip() in ("", "None")):
            missing.append(field)
    return missing


def dedup_key(txn: dict) -> str:
    """중복 탐지용 해시 키를 생성한다 (동일 날짜+금액+거래처)."""
    parts = f"{txn.get('date')}|{txn.get('amount')}|{txn.get('counterparty')}"
    return hashlib.md5(parts.encode()).hexdigest()


def run(input_dir: str, month: str | None = None) -> None:
    """FELIX 메인 파이프라인."""
    root = get_project_root()
    raw_dir = Path(input_dir) if os.path.isabs(input_dir) else root / input_dir

    print(f"[FELIX] 스캔 시작: {raw_dir}")

    files = scan_raw_files(raw_dir)
    if not files:
        print("[FELIX] 처리할 파일이 없습니다.")
        return

    print(f"[FELIX] 발견 파일 {len(files)}건")

    all_transactions: list[dict] = []
    missing_alerts: list[dict] = []

    for filepath in files:
        print(f"  → {filepath.name} ... ", end="")
        rows = read_data_file(filepath)
        if not rows:
            print("(빈 파일 또는 읽기 실패)")
            continue

        # 헤더 기반 파일 유형 검출
        header = list(rows[0].keys()) if rows else None
        ftype = detect_file_type(filepath.name, header)
        print(f"[{ftype}] {len(rows)}행")

        parser = PARSERS.get(ftype)
        if parser is None:
            print(f"    [SKIP] 알 수 없는 파일 유형: {ftype}")
            continue

        parsed = parser(rows, filepath.name)
        all_transactions.extend(parsed)

    # 월 필터링
    if month:
        all_transactions = [
            t for t in all_transactions
            if t.get("date") and t["date"].startswith(month)
        ]

    # 누락 필드 탐지
    clean_transactions: list[dict] = []
    for txn in all_transactions:
        missing = check_missing_fields(txn)
        if missing:
            missing_alerts.append({
                "source_file": txn.get("source_file"),
                "date": txn.get("date"),
                "missing_fields": missing,
                "raw_data": txn.get("raw_data", {}),
            })
        clean_transactions.append(txn)

    # 중복 제거
    seen: set[str] = set()
    deduped: list[dict] = []
    dup_count = 0
    for txn in clean_transactions:
        key = dedup_key(txn)
        if key in seen:
            dup_count += 1
            continue
        seen.add(key)
        deduped.append(txn)

    if dup_count:
        print(f"[FELIX] 중복 제거: {dup_count}건")

    # ID 부여
    date_seq: dict[str, int] = {}
    for txn in deduped:
        d = txn.get("date") or "unknown"
        date_seq[d] = date_seq.get(d, 0) + 1
        txn["id"] = make_txn_id(d, date_seq[d])

    # 통계
    income_count = sum(1 for t in deduped if t["type"] == "income")
    expense_count = sum(1 for t in deduped if t["type"] == "expense")

    # 출력
    month_label = month or datetime.now().strftime("%Y-%m")
    out_dir = root / "data" / "processed" / "transactions"
    out_path = out_dir / f"{month_label}_transactions.json"
    save_json(deduped, out_path)
    print(f"[FELIX] 저장: {out_path}")

    # 누락 알림
    if missing_alerts:
        today_str = datetime.now().strftime("%Y%m%d")
        alert_path = root / "outputs" / "alerts" / f"felix_missing_{today_str}.json"
        save_json(missing_alerts, alert_path)
        print(f"[FELIX] 누락 알림: {alert_path}")

    # 요약
    print(
        f"\n[FELIX] 총 {len(deduped)}건 처리, "
        f"매출 {income_count}건, 매입 {expense_count}건, "
        f"누락 {len(missing_alerts)}건"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="FELIX - 데이터 수집 및 정제 에이전트"
    )
    parser.add_argument(
        "--input",
        default="data/raw/",
        help="입력 디렉터리 경로 (기본: data/raw/)",
    )
    parser.add_argument(
        "--month",
        default=None,
        help="필터링할 월 (예: 2026-03). 미지정 시 전체 처리.",
    )
    args = parser.parse_args()
    run(args.input, args.month)


if __name__ == "__main__":
    main()
