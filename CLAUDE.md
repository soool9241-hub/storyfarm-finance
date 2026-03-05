# StoryFarm Finance - 에이전트 공통 지침서

## 공통 규칙
- 모든 금액은 원화(₩) 기준, 정수로 처리
- 공방 수익과 펜션(달팽이아지트) 수익은 반드시 분리 태깅: business_type: "workshop" | "pension"
- 재료비 기록 시 소재 명시: AL6061, SUS304, MDF, 기타
- 이상 수치 발견 시 즉시 outputs/alerts/ 에 기록
- 모든 에이전트 결과는 JSON으로 data/processed/ 에 저장
- 날짜 형식: YYYY-MM-DD 통일
- 에러 발생 시 작업 중단하지 말고 로그 남기고 계속 진행

## 에이전트 실행 순서
Phase 1: FELIX → 데이터 수집·정제
Phase 2: LUNA + MARCO + ASSET → 회계·원가·자산
Phase 3: CASH → 현금흐름
Phase 4: DEBT + TAX + PROFIT → 채무·세무·수익성
Phase 5: REPORT → 리포트 생성
Phase 6: SOL-CFO → 총괄 브리핑
