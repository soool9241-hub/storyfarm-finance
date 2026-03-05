"""Vercel Serverless Function - 에이전트 목록 API (조회 전용)"""
import json
from http.server import BaseHTTPRequestHandler

AGENTS = {
    "felix": {"name": "FELIX", "desc": "데이터 수집 및 정제", "phase": 1, "args": "--input data/raw/ --month 2026-03"},
    "luna": {"name": "LUNA", "desc": "복식부기 자동 분개", "phase": 2, "args": "--month 2026-03"},
    "marco": {"name": "MARCO", "desc": "수주별 원가 분석", "phase": 2, "args": "--month 2026-03"},
    "asset": {"name": "ASSET", "desc": "자산 감가상각 관리", "phase": 2, "args": "--month 2026-03"},
    "cash": {"name": "CASH", "desc": "30일 현금흐름 예측", "phase": 3, "args": "--days 30 --scenario base"},
    "debt": {"name": "DEBT", "desc": "채무 상환 시뮬레이션", "phase": 4, "args": "--simulate"},
    "tax": {"name": "TAX", "desc": "부가세/소득세 준비", "phase": 4, "args": "--quarter 2026-Q1 --prepare"},
    "profit": {"name": "PROFIT", "desc": "수익성 분석", "phase": 4, "args": "--month 2026-03"},
    "report": {"name": "REPORT", "desc": "재무제표 생성", "phase": 5, "args": "--month 2026-03 --full"},
    "sol_cfo": {"name": "SOL-CFO", "desc": "전체 파이프라인 총괄", "phase": 6, "args": "--monthly-close --month 2026-03"},
}


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        result = {}
        for key, info in AGENTS.items():
            result[key] = {**info, "status": "idle", "output": "", "started_at": None, "finished_at": None}
        body = json.dumps(result, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)
