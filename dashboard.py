"""
storyfarm-finance / dashboard.py
스토리팜 재무 대시보드 서버
- 에이전트 산출물 JSON을 읽어 API로 제공
- 웹에서 10개 에이전트 개별/전체 실행 가능
- 브라우저에서 http://localhost:8787 접속
"""

import base64
import csv
import http.server
import io
import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(os.path.dirname(os.path.abspath(__file__)))
PROCESSED = ROOT / "data" / "processed"
REPORTS = ROOT / "outputs" / "reports"
DATA = ROOT / "data"

sys.path.insert(0, str(ROOT))
from tools.calculator import calc_interest

# ── 에이전트 실행 상태 관리 ──
agent_status = {}  # { agent_name: { status, output, started_at, finished_at } }
agent_lock = threading.Lock()

AGENTS = {
    "felix": {
        "name": "FELIX",
        "desc": "데이터 수집 및 정제",
        "file": "agents/felix.py",
        "default_args": ["--input", "data/raw/", "--month", "2026-03"],
        "phase": 1,
    },
    "luna": {
        "name": "LUNA",
        "desc": "복식부기 자동 분개",
        "file": "agents/luna.py",
        "default_args": ["--month", "2026-03"],
        "phase": 2,
    },
    "marco": {
        "name": "MARCO",
        "desc": "수주별 원가 분석",
        "file": "agents/marco.py",
        "default_args": ["--month", "2026-03"],
        "phase": 2,
    },
    "asset": {
        "name": "ASSET",
        "desc": "자산 감가상각 관리",
        "file": "agents/asset.py",
        "default_args": ["--month", "2026-03"],
        "phase": 2,
    },
    "cash": {
        "name": "CASH",
        "desc": "30일 현금흐름 예측",
        "file": "agents/cash.py",
        "default_args": ["--days", "30", "--scenario", "base"],
        "phase": 3,
    },
    "debt": {
        "name": "DEBT",
        "desc": "채무 상환 시뮬레이션",
        "file": "agents/debt.py",
        "default_args": ["--simulate"],
        "phase": 4,
    },
    "tax": {
        "name": "TAX",
        "desc": "부가세/소득세 준비",
        "file": "agents/tax.py",
        "default_args": ["--quarter", "2026-Q1", "--prepare"],
        "phase": 4,
    },
    "profit": {
        "name": "PROFIT",
        "desc": "수익성 분석",
        "file": "agents/profit.py",
        "default_args": ["--month", "2026-03"],
        "phase": 4,
    },
    "report": {
        "name": "REPORT",
        "desc": "재무제표 생성",
        "file": "agents/report.py",
        "default_args": ["--month", "2026-03", "--full"],
        "phase": 5,
    },
    "sol_cfo": {
        "name": "SOL-CFO",
        "desc": "전체 파이프라인 총괄",
        "file": "agents/sol_cfo.py",
        "default_args": ["--monthly-close", "--month", "2026-03"],
        "phase": 6,
    },
}


def _run_agent_thread(agent_key: str, custom_args: str = None):
    """에이전트를 백그라운드 스레드에서 실행."""
    info = AGENTS[agent_key]

    # 사용자가 직접 입력한 인자가 있으면 사용, 없으면 기본값
    if custom_args and custom_args.strip():
        args = custom_args.strip().split()
    else:
        args = info["default_args"]

    with agent_lock:
        agent_status[agent_key] = {
            "status": "running",
            "output": "",
            "args_used": " ".join(args),
            "started_at": time.strftime("%H:%M:%S"),
            "finished_at": None,
        }

    try:
        env = os.environ.copy()
        env["PYTHONUTF8"] = "1"
        result = subprocess.run(
            [sys.executable, str(ROOT / info["file"])] + args,
            capture_output=True,
            text=True,
            cwd=str(ROOT),
            env=env,
            timeout=120,
        )
        output = result.stdout
        if result.stderr:
            output += "\n[STDERR]\n" + result.stderr
        status = "success" if result.returncode == 0 else "error"
    except subprocess.TimeoutExpired:
        output = "[TIMEOUT] 2분 초과로 중단됨"
        status = "error"
    except Exception as e:
        output = f"[ERROR] {str(e)}"
        status = "error"

    with agent_lock:
        agent_status[agent_key] = {
            "status": status,
            "output": output,
            "started_at": agent_status[agent_key]["started_at"],
            "finished_at": time.strftime("%H:%M:%S"),
        }


def _load(path):
    """JSON 파일 로드 (없으면 None)."""
    if not path.exists():
        return None
    with open(str(path), "r", encoding="utf-8") as f:
        return json.load(f)


def _find_month_file(directory: Path, month: str, keyword: str = ""):
    """디렉토리에서 month와 keyword가 포함된 JSON 파일 로드."""
    if not directory.exists():
        return None
    for fpath in sorted(directory.glob("*.json")):
        if month in fpath.stem:
            if keyword and keyword not in fpath.stem:
                continue
            return _load(fpath)
    return None


def collect_dashboard_data(month: str = "2026-03") -> dict:
    """모든 에이전트 산출물을 모아 대시보드 데이터 구성."""

    # 1. 계정별원장
    ledger = _find_month_file(PROCESSED / "ledger", month, "계정별원장") or {}

    # 2. 자산현황
    assets_data = _find_month_file(PROCESSED / "assets", month) or {}
    dep_list = assets_data.get("depreciation", [])

    # 3. 채무
    debts_raw = _load(DATA / "debts.json") or {}
    debts = debts_raw.get("debts", []) if isinstance(debts_raw, dict) else debts_raw

    # 4. 수익성분석
    profit_data = _find_month_file(PROCESSED / "profit", month) or {}

    # 5. 현금흐름 예측
    cashflow = _find_month_file(PROCESSED / "cashflow", month) or {}

    # 6. 수주별 원가
    cost_data = _find_month_file(PROCESSED / "cost", month, "수주별원가") or {}

    # 7. 마진분석
    margin_data = _find_month_file(PROCESSED / "cost", month, "마진분석") or {}

    # 8. 상환 시뮬레이션
    sim_data = _load(PROCESSED / "debt" / "상환시뮬레이션.json") or {}

    # ── 손익 데이터 (계정별원장에서) ──
    revenue_items = {}
    expense_items = {}
    total_revenue = 0
    total_cogs = 0
    total_sga = 0

    for acct_name, acct in ledger.items():
        acct_type = acct.get("type", "")
        debit = int(acct.get("total_debit", 0))
        credit = int(acct.get("total_credit", 0))

        if acct_type == "revenue" or "매출" in acct_name:
            revenue_items[acct_name] = credit
            total_revenue += credit
        elif acct_type == "cogs":
            expense_items[acct_name] = debit
            total_cogs += debit
        elif acct_type == "sga":
            expense_items[acct_name] = debit
            total_sga += debit

    # 이자비용 추정
    total_interest = 0
    for d in debts:
        bal = d.get("current_balance", 0)
        rate = d.get("interest_rate", 0)
        if bal and rate:
            total_interest += calc_interest(int(bal), float(rate))

    gross_profit = total_revenue - total_cogs
    operating_income = gross_profit - total_sga
    net_income = operating_income - total_interest

    # ── 자산 데이터 ──
    asset_cards = []
    total_book_value = 0
    for item in dep_list:
        bv = item.get("book_value", 0)
        total_book_value += bv
        asset_cards.append({
            "name": item.get("asset_name", ""),
            "acquisition_cost": item.get("acquisition_cost", 0),
            "book_value": bv,
            "accumulated_dep": item.get("accumulated_depreciation", 0),
            "monthly_dep": item.get("monthly_depreciation", 0),
            "life_pct": item.get("life_elapsed_pct", 0),
            "remaining_months": item.get("remaining_months", 0),
        })

    # ── 채무 데이터 ──
    total_debt = sum(d.get("current_balance", 0) for d in debts)
    debt_cards = []
    for d in debts:
        bal = d.get("current_balance", 0)
        rate = d.get("interest_rate", 0)
        debt_cards.append({
            "name": d.get("name", ""),
            "balance": bal,
            "rate_pct": round(rate * 100, 1),
            "monthly_payment": d.get("monthly_payment", 0),
            "monthly_interest": calc_interest(bal, rate) if bal and rate else 0,
            "original": d.get("original_amount", 0),
            "paid_pct": round((1 - bal / d["original_amount"]) * 100, 1)
            if d.get("original_amount") else 0,
        })

    # ── 현금흐름 예측 ──
    forecast_days = []
    danger_zones = []
    if cashflow:
        forecast_days = cashflow.get("daily_forecast", cashflow.get("forecast", []))
        danger_zones = cashflow.get("danger_zones", [])

    # ── 수주 원가 ──
    orders = cost_data.get("orders", []) if isinstance(cost_data, dict) else []
    margin_summary = margin_data.get("summary", {}) if isinstance(margin_data, dict) else {}

    # ── 펜션 분석 ──
    pension = profit_data.get("pension_analysis", {})

    # ── 상환 시뮬레이션 ──
    sim_scenarios = sim_data.get("scenarios", {})
    sim_summary = []
    for key, sc in sim_scenarios.items():
        sim_summary.append({
            "name": sc.get("description", key),
            "total_months": sc.get("total_months", 0),
            "total_interest": sc.get("total_interest_paid", 0),
            "payoff_date": sc.get("payoff_date_est", ""),
        })

    return {
        "month": month,
        "summary": {
            "total_revenue": total_revenue,
            "total_cogs": total_cogs,
            "gross_profit": gross_profit,
            "total_sga": total_sga,
            "operating_income": operating_income,
            "interest_expense": total_interest,
            "net_income": net_income,
            "total_assets": total_book_value,
            "total_debt": total_debt,
            "debt_ratio": round(total_debt / total_book_value * 100, 1)
            if total_book_value > 0 else 0,
        },
        "revenue_breakdown": revenue_items,
        "expense_breakdown": expense_items,
        "assets": asset_cards,
        "debts": debt_cards,
        "cashflow_forecast": forecast_days[:30],
        "danger_zones": danger_zones,
        "orders": orders[:10],
        "margin_summary": margin_summary,
        "pension": pension,
        "repayment_sim": sim_summary,
    }


def handle_excel_upload(filename: str, data_b64: str) -> dict:
    """엑셀/CSV 파일 업로드 처리: base64 → data/raw/ 저장 → 미리보기 반환."""
    raw_dir = ROOT / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    file_bytes = base64.b64decode(data_b64)
    save_path = raw_dir / filename
    with open(str(save_path), "wb") as f:
        f.write(file_bytes)

    # CSV 미리보기 파싱
    preview_rows = []
    headers = []
    try:
        text = None
        for enc in ["utf-8", "euc-kr", "cp949"]:
            try:
                text = file_bytes.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text:
            reader = csv.reader(io.StringIO(text))
            for i, row in enumerate(reader):
                if i == 0:
                    headers = row
                elif i <= 20:
                    preview_rows.append(row)
    except Exception as e:
        return {"ok": True, "path": str(save_path), "error": f"미리보기 파싱 실패: {e}", "headers": [], "rows": []}

    return {
        "ok": True,
        "path": str(save_path),
        "filename": filename,
        "headers": headers,
        "rows": preview_rows,
        "total_rows": len(preview_rows),
        "message": f"{filename} 저장 완료 ({len(preview_rows)}건)",
    }


def handle_receipt_save(items: list) -> dict:
    """OCR 인식 결과를 거래 데이터로 저장."""
    raw_dir = ROOT / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"영수증스캔_{timestamp}.csv"
    save_path = raw_dir / filename

    with open(str(save_path), "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["날짜", "적요", "금액", "구분", "비고"])
        for item in items:
            writer.writerow([
                item.get("date", ""),
                item.get("description", ""),
                item.get("amount", ""),
                item.get("type", "지출"),
                item.get("note", "영수증스캔"),
            ])

    return {
        "ok": True,
        "path": str(save_path),
        "filename": filename,
        "count": len(items),
        "message": f"영수증 {len(items)}건 저장 완료 → {filename}",
    }


def generate_export_csv() -> str:
    """전체 거래내역을 CSV 문자열로 생성."""
    tx_dir = PROCESSED / "transactions"
    rows = []
    if tx_dir.exists():
        for fpath in sorted(tx_dir.glob("*.json")):
            try:
                data = json.loads(fpath.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    rows.extend(data)
                elif isinstance(data, dict):
                    rows.append(data)
            except Exception:
                pass

    output = io.StringIO()
    if rows:
        keys = list(rows[0].keys())
        writer = csv.DictWriter(output, fieldnames=keys, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    else:
        output.write("데이터 없음\n")
    return output.getvalue()


def generate_export_all_json() -> dict:
    """전체 리포트를 하나의 JSON으로 통합."""
    result = {}
    for subdir in ["ledger", "cost", "assets", "cashflow", "debt", "profit", "transactions"]:
        target = PROCESSED / subdir
        if not target.exists():
            continue
        result[subdir] = {}
        for fpath in sorted(target.glob("*.json")):
            try:
                result[subdir][fpath.stem] = json.loads(fpath.read_text(encoding="utf-8"))
            except Exception:
                pass

    if REPORTS.exists():
        result["reports"] = {}
        for fpath in sorted(REPORTS.glob("*.json")):
            try:
                result["reports"][fpath.stem] = json.loads(fpath.read_text(encoding="utf-8"))
            except Exception:
                pass

    return result


def get_agents_info() -> dict:
    """에이전트 목록과 현재 실행 상태 반환."""
    result = {}
    for key, info in AGENTS.items():
        with agent_lock:
            status = agent_status.get(key, {"status": "idle", "output": "", "started_at": None, "finished_at": None})
        result[key] = {
            "name": info["name"],
            "desc": info["desc"],
            "phase": info["phase"],
            "args": " ".join(info["default_args"]),
            **status,
        }
    return result


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """대시보드 HTTP 요청 처리."""

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/" or path == "/index.html":
            self._serve_file(ROOT / "dashboard" / "index.html", "text/html")
        elif path == "/api/data":
            self._serve_json(collect_dashboard_data("2026-03"))
        elif path.startswith("/api/data/"):
            month = path.split("/")[-1]
            self._serve_json(collect_dashboard_data(month))
        elif path == "/api/agents":
            self._serve_json(get_agents_info())
        elif path.startswith("/api/agent/output/"):
            agent_key = path.split("/")[-1]
            with agent_lock:
                status = agent_status.get(agent_key, {"status": "idle", "output": ""})
            self._serve_json(status)
        elif path == "/api/export/csv":
            csv_data = generate_export_csv()
            encoded = csv_data.encode("utf-8-sig")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", "attachment; filename=storyfarm_transactions.csv")
            self.send_header("Content-Length", len(encoded))
            self.end_headers()
            self.wfile.write(encoded)
        elif path == "/api/export/json":
            self._serve_json(generate_export_all_json())
        elif path.startswith("/api/export/report/"):
            report_name = path.split("/")[-1]
            found = None
            if REPORTS.exists():
                for fpath in REPORTS.glob("*.json"):
                    if report_name in fpath.stem:
                        found = json.loads(fpath.read_text(encoding="utf-8"))
                        break
            if found:
                self._serve_json(found)
            else:
                self._serve_json({"error": f"리포트 없음: {report_name}"})
        elif path == "/api/files/raw":
            # data/raw/ 파일 목록
            raw_dir = ROOT / "data" / "raw"
            files = []
            if raw_dir.exists():
                for f in sorted(raw_dir.iterdir()):
                    if f.is_file():
                        files.append({
                            "name": f.name,
                            "size": f.stat().st_size,
                            "modified": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M"),
                        })
            self._serve_json(files)
        else:
            self.send_error(404)

    def _read_body(self):
        """POST body를 읽어 JSON으로 파싱."""
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/agent/run/"):
            agent_key = path.split("/")[-1]
            if agent_key not in AGENTS:
                self._serve_json({"error": f"알 수 없는 에이전트: {agent_key}"})
                return

            with agent_lock:
                current = agent_status.get(agent_key, {})
                if current.get("status") == "running":
                    self._serve_json({"error": f"{AGENTS[agent_key]['name']} 이미 실행 중"})
                    return

            # 사용자 커스텀 인자 받기
            body = self._read_body()
            custom_args = body.get("args", None)

            thread = threading.Thread(
                target=_run_agent_thread, args=(agent_key, custom_args), daemon=True
            )
            thread.start()
            self._serve_json({"ok": True, "message": f"{AGENTS[agent_key]['name']} 실행 시작"})

        elif path == "/api/upload/excel":
            body = self._read_body()
            filename = body.get("filename", "upload.csv")
            data_b64 = body.get("data", "")
            if not data_b64:
                self._serve_json({"error": "파일 데이터 없음"})
                return
            result = handle_excel_upload(filename, data_b64)
            self._serve_json(result)

        elif path == "/api/receipt/save":
            body = self._read_body()
            items = body.get("items", [])
            if not items:
                self._serve_json({"error": "저장할 항목이 없습니다"})
                return
            result = handle_receipt_save(items)
            self._serve_json(result)

        elif path == "/api/agent/run-all":
            with agent_lock:
                current = agent_status.get("sol_cfo", {})
                if current.get("status") == "running":
                    self._serve_json({"error": "SOL-CFO 이미 실행 중"})
                    return
            thread = threading.Thread(target=_run_agent_thread, args=("sol_cfo", None), daemon=True)
            thread.start()
            self._serve_json({"ok": True, "message": "SOL-CFO 전체 파이프라인 실행 시작"})

        else:
            self.send_error(404)

    def _serve_file(self, filepath, content_type):
        try:
            with open(str(filepath), "r", encoding="utf-8") as f:
                content = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", len(content))
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404, f"File not found: {filepath}")

    def _serve_json(self, data):
        content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        encoded = content.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(encoded))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format, *args):
        print(f"[대시보드] {args[0]}")


def main():
    port = 8787
    print("=" * 50)
    print("  스토리팜 재무 대시보드")
    print("=" * 50)
    print(f"  http://localhost:{port}")
    print(f"  API: http://localhost:{port}/api/data")
    print(f"  에이전트: http://localhost:{port}/api/agents")
    print("  Ctrl+C 로 종료")
    print("=" * 50)

    server = http.server.HTTPServer(("", port), DashboardHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[대시보드] 서버 종료")
        server.server_close()


if __name__ == "__main__":
    main()
