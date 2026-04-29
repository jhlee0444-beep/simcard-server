import os
import csv
import io
import requests
import logging
import threading
from datetime import datetime
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================
# 설정 (Render 환경변수로 관리)
# =============================================
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://sdyefkrufoylwyjxgywd.supabase.co')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY', '')
TABLE_NAME = 'simcards'
BATCH_SIZE = 1000

# 10개 시트 CSV 링크
SHEETS = {
    '기본':        'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=2135846874&single=true&output=csv',
    '재사용공심':  'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=965015374&single=true&output=csv',
    '창성정보통신':'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=1551352435&single=true&output=csv',
    'JCB':         'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=2078441337&single=true&output=csv',
    '차이나텔레콤':'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=585592835&single=true&output=csv',
    '보스그룹':    'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=84263069&single=true&output=csv',
    'PPL':         'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=1257657731&single=true&output=csv',
    '코드':        'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=1285137321&single=true&output=csv',
    '포스원':      'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=2012098034&single=true&output=csv',
    '한유망':      'https://docs.google.com/spreadsheets/d/e/2PACX-1vS96r7U61fAYW8skGXcV-mJ9Xo890SUPaLPuX3DzgohGwIZ4_kezL8_jBMnsKhrgihRbb5c0lJDb4vU/pub?gid=707329122&single=true&output=csv',
}

supabase_headers = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json',
    'Prefer': 'return=minimal'
}

sync_status = {
    'last_sync': None,
    'last_count': 0,
    'status': '대기 중',
    'error': None,
    'running': False,
    'progress': ''
}

# =============================================
# CSV 스트리밍으로 읽기
# =============================================
def fetch_sheet(sheet_name, csv_url):
    logger.info(f'[{sheet_name}] CSV 다운로드 중...')
    resp = requests.get(csv_url, timeout=300)
    resp.raise_for_status()

    content = resp.content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for row in reader:
        # 빈 행 스킵
        if all(v.strip() == '' for v in row.values()):
            continue
        cleaned = {}
        for k, v in row.items():
            if k:  # 빈 컬럼명 제외
                cleaned[k.strip()] = v.strip()
        cleaned['시트명'] = sheet_name
        rows.append(cleaned)

    logger.info(f'[{sheet_name}] {len(rows):,}건 로드 완료')
    return rows

# =============================================
# Supabase 배치 삽입
# =============================================
def insert_batch(rows):
    if not rows:
        return True
    resp = requests.post(
        f'{SUPABASE_URL}/rest/v1/{TABLE_NAME}',
        headers=supabase_headers,
        json=rows,
        timeout=60
    )
    if resp.status_code != 201:
        logger.error(f'삽입 오류: {resp.status_code} / {resp.text[:300]}')
        return False
    return True

# =============================================
# 메인 동기화
# =============================================
def sync_all():
    if sync_status['running']:
        logger.info('이미 동기화 중')
        return

    sync_status['running'] = True
    sync_status['status'] = '동기화 중...'
    sync_status['error'] = None
    sync_status['progress'] = ''
    logger.info('=== 동기화 시작 ===')

    try:
        # 1. 모든 시트 CSV 수집
        all_rows = []
        for sheet_name, csv_url in SHEETS.items():
            try:
                sync_status['progress'] = f'{sheet_name} 읽는 중...'
                rows = fetch_sheet(sheet_name, csv_url)
                all_rows.extend(rows)
            except Exception as e:
                logger.error(f'[{sheet_name}] 오류: {e}')
                continue

        total = len(all_rows)
        logger.info(f'전체 합계: {total:,}건')
        sync_status['progress'] = f'총 {total:,}건 수집 완료, DB 저장 중...'

        if total == 0:
            sync_status['status'] = '데이터 없음'
            return

        # 2. 기존 데이터 삭제
        logger.info('기존 데이터 삭제 중...')
        requests.delete(
            f'{SUPABASE_URL}/rest/v1/{TABLE_NAME}?id=gte.0',
            headers=supabase_headers,
            timeout=60
        )

        # 3. 배치 삽입
        inserted = 0
        for i in range(0, total, BATCH_SIZE):
            batch = all_rows[i:i + BATCH_SIZE]
            if insert_batch(batch):
                inserted += len(batch)
                sync_status['progress'] = f'{inserted:,}/{total:,}건 저장 완료'
                logger.info(f'{inserted:,}/{total:,}건 완료')

        sync_status['last_sync'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sync_status['last_count'] = inserted
        sync_status['status'] = f'완료'
        sync_status['progress'] = f'{inserted:,}건 동기화됨'
        logger.info(f'✅ 동기화 완료: {inserted:,}건')

    except Exception as e:
        logger.error(f'동기화 오류: {e}')
        sync_status['status'] = '오류 발생'
        sync_status['error'] = str(e)
    finally:
        sync_status['running'] = False

# =============================================
# API
# =============================================
@app.route('/')
def index():
    return jsonify({
        'service': '심카드 DB 동기화 서버',
        'status': sync_status['status'],
        'progress': sync_status['progress'],
        'last_sync': sync_status['last_sync'],
        'last_count': f"{sync_status['last_count']:,}건",
        'running': sync_status['running'],
        'error': sync_status['error'],
        'sheets': list(SHEETS.keys())
    })

@app.route('/health')
def health():
    # UptimeRobot이 5분마다 핑 → 슬립 방지
    return jsonify({'status': 'ok', 'time': datetime.now().isoformat()})

@app.route('/sync', methods=['GET', 'POST'])
def manual_sync():
    if sync_status['running']:
        return jsonify({'message': '이미 동기화 중', 'progress': sync_status['progress']})
    thread = threading.Thread(target=sync_all, daemon=True)
    thread.start()
    return jsonify({'message': '동기화 시작됨! 완료까지 수분 소요될 수 있습니다.'})

@app.route('/status')
def status():
    return jsonify(sync_status)

# =============================================
# 스케줄러: 매일 새벽 3시 자동 실행
# =============================================
scheduler = BackgroundScheduler(timezone='Asia/Seoul')
scheduler.add_job(sync_all, 'cron', hour=3, minute=0)
scheduler.start()

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
