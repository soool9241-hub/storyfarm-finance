"""
storyfarm-finance / tools / formatter.py
출력 포맷팅 유틸리티 - 테이블 출력, 브리핑 박스, JSON 입출력, 경로 관리
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def get_project_root() -> Path:
    """
    storyfarm-finance 프로젝트 루트 경로를 반환.
    이 파일 위치(tools/)의 상위 디렉토리를 기준으로 한다.

    Returns:
        프로젝트 루트 Path 객체
    """
    return Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def ensure_dir(path: str) -> str:
    """
    디렉토리가 없으면 생성. 이미 있으면 무시.

    Args:
        path: 생성할 디렉토리 경로

    Returns:
        생성된(또는 기존) 디렉토리의 절대 경로
    """
    abs_path = os.path.abspath(path)
    os.makedirs(abs_path, exist_ok=True)
    return abs_path


def print_separator(char: str = "\u2501", length: int = 50) -> None:
    """
    구분선 출력.

    Args:
        char: 구분선 문자 (기본: ━)
        length: 길이
    """
    print(char * length)


def _display_width(s: str) -> int:
    """문자열의 표시 너비 계산 (한글/CJK=2, 그 외=1)."""
    width = 0
    for ch in str(s):
        if ord(ch) > 0x7F:
            width += 2
        else:
            width += 1
    return width


def _pad_to_width(s: str, target_width: int) -> str:
    """표시 너비 기준으로 오른쪽 패딩."""
    current = _display_width(s)
    return str(s) + " " * max(0, target_width - current)


def print_table(headers: List[str], rows: List[List[Any]]) -> None:
    """
    정렬된 테이블을 콘솔에 출력.
    각 컬럼 너비를 내용에 맞게 자동 조정한다.

    Args:
        headers: 컬럼 헤더 리스트
        rows: 행 데이터 (2차원 리스트)
    """
    if not headers:
        return

    all_rows = [headers] + [[str(cell) for cell in row] for row in rows]

    # 컬럼 수 통일
    max_cols = len(headers)
    for row in all_rows:
        while len(row) < max_cols:
            row.append("")

    # 각 컬럼 최대 너비
    col_widths = []
    for col_idx in range(max_cols):
        max_w = max(_display_width(row[col_idx]) for row in all_rows)
        col_widths.append(max_w)

    # 헤더 출력
    header_line = " | ".join(
        _pad_to_width(headers[i], col_widths[i]) for i in range(max_cols)
    )
    print(header_line)

    # 구분선
    sep_parts = ["\u2500" * w for w in col_widths]
    print("-+-".join(sep_parts))

    # 데이터 행 출력
    for row in all_rows[1:]:
        line = " | ".join(
            _pad_to_width(row[i], col_widths[i]) for i in range(max_cols)
        )
        print(line)


def print_briefing(title: str, lines: List[str]) -> None:
    """
    포맷된 브리핑 박스를 출력.

    Args:
        title: 브리핑 제목
        lines: 내용 라인 리스트
    """
    all_texts = [title] + lines
    max_width = max(_display_width(t) for t in all_texts)
    box_width = max(max_width + 4, 40)

    # 상단 테두리
    print("\u250c" + "\u2500" * (box_width - 2) + "\u2510")

    # 제목
    title_pad = box_width - 4 - _display_width(title)
    print("\u2502 " + title + " " * max(0, title_pad) + " \u2502")

    # 제목 아래 구분선
    print("\u251c" + "\u2500" * (box_width - 2) + "\u2524")

    # 내용
    for line in lines:
        line_pad = box_width - 4 - _display_width(line)
        print("\u2502 " + line + " " * max(0, line_pad) + " \u2502")

    # 하단 테두리
    print("\u2514" + "\u2500" * (box_width - 2) + "\u2518")


def save_json(data: Any, filepath: str) -> str:
    """
    딕셔너리/리스트를 JSON 파일로 저장.
    한글이 이스케이프되지 않도록 ensure_ascii=False 사용.

    Args:
        data: 저장할 데이터
        filepath: 저장 경로

    Returns:
        저장된 파일의 절대 경로
    """
    abs_path = os.path.abspath(str(filepath))
    ensure_dir(os.path.dirname(abs_path))

    with open(abs_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    return abs_path


def load_json(filepath) -> Any:
    """
    JSON 파일을 로드.

    Args:
        filepath: JSON 파일 경로 (str 또는 Path)

    Returns:
        파싱된 데이터 (dict 또는 list)
    """
    abs_path = os.path.abspath(str(filepath))
    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"파일을 찾을 수 없습니다: {abs_path}")

    with open(abs_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ── 테스트 블록 ──────────────────────────────────────────────

if __name__ == "__main__":
    print("=== formatter.py 단위 테스트 ===\n")

    # 1. get_project_root
    root = get_project_root()
    print(f"[INFO] 프로젝트 루트: {root}")
    assert os.path.isdir(root), "프로젝트 루트가 존재하지 않음"
    print("[PASS] get_project_root\n")

    # 2. ensure_dir
    test_dir = os.path.join(root, "outputs", "_test_temp")
    result_dir = ensure_dir(test_dir)
    assert os.path.isdir(result_dir), "디렉토리 생성 실패"
    os.rmdir(result_dir)  # 정리
    print("[PASS] ensure_dir\n")

    # 3. print_separator
    print("[테스트] print_separator:")
    print_separator()
    print_separator("=", 30)
    print()

    # 4. print_table
    print("[테스트] print_table:")
    headers = ["항목", "금액", "비율"]
    rows = [
        ["CNC 머신", "₩30,000,000", "60%"],
        ["레이저커터", "₩15,000,000", "30%"],
        ["CNC 라우터", "₩8,000,000", "10%"],
    ]
    print_table(headers, rows)
    print()

    # 5. print_briefing
    print("[테스트] print_briefing:")
    print_briefing(
        "2026년 3월 재무 브리핑",
        [
            "총자산: ₩53,000,000",
            "총부채: ₩18,000,000",
            "순자산: ₩35,000,000",
            "",
            "이번 달 주의사항:",
            "  - 카드론 이자율 19.5% (고금리)",
            "  - 국민은행 만기 2026-10-31",
        ]
    )
    print()

    # 6. save_json / load_json 왕복 테스트
    test_data = {
        "테스트": True,
        "금액": 1234567,
        "항목": ["CNC", "레이저", "라우터"],
    }
    test_json_path = os.path.join(root, "outputs", "_test_temp.json")
    saved_path = save_json(test_data, test_json_path)
    loaded = load_json(saved_path)
    assert loaded == test_data, f"JSON 왕복 실패: {loaded}"
    os.remove(saved_path)  # 정리
    print("[PASS] save_json / load_json 왕복 테스트\n")

    print("모든 테스트 통과!")
