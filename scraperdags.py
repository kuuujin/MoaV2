from __future__ import annotations

import json
import pendulum
import re
import time

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
except ImportError:
    print("Selenium dependencies not found. Please ensure selenium and its webdriver are installed.")
    print("This DAG requires a compatible Chrome browser and ChromeDriver on the Airflow worker.")


def parse_timestamp(timestamp_str):
    """주어진 타임스탬프 문자열을 파싱하여 datetime 객체를 반환합니다."""
    now = datetime.now()
    timestamp_str = timestamp_str.replace(" ", "")  # 공백 제거
    if "방금" in timestamp_str:
        return now
    elif "분전" in timestamp_str:
        minutes = int(re.search(r'(\d+)분전', timestamp_str).group(1))
        return now - timedelta(minutes=minutes)
    elif "시간전" in timestamp_str:
        hours = int(re.search(r'(\d+)시간전', timestamp_str).group(1))
        return now - timedelta(hours=hours)
    elif "일전" in timestamp_str:
        days = int(re.search(r'(\d+)일전', timestamp_str).group(1))
        return now - timedelta(days=days)
    elif "주전" in timestamp_str:
        weeks = int(re.search(r'(\d+)주전', timestamp_str).group(1))
        return now - timedelta(weeks=weeks)
    elif "개월전" in timestamp_str:
        months = int(re.search(r'(\d+)개월전', timestamp_str).group(1))
        # 정확한 월 계산은 복잡하므로 대략적인 일수로 계산 (30일/월)
        return now - timedelta(days=months * 30)
    else:
        return None

def scrape_and_process_data(**kwargs):
    bucket_name = 'moastorage'
    blob_name = 'data/hotdeal.json'
    
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default') # Airflow Connection ID

    existing_data = []
    # GCS에서 기존 데이터 로드 시도
    try:
        # object_exists 메서드를 사용하여 blob 존재 여부 확인
        if gcs_hook.exists(bucket_name=bucket_name, object_name=blob_name):
            #data_str = gcs_hook.download_as_string(bucket_name=bucket_name, object_name=blob_name).decode('utf-8')
            data_str = gcs_hook.download(bucket_name=bucket_name, object_name=blob_name).decode('utf-8')
            existing_data = json.loads(data_str)
            print(f"Loaded {len(existing_data)} existing items from GCS.")
        else:
            print(f"Blob {blob_name} does not exist in bucket {bucket_name}. Starting with empty data.")
    except Exception as e:
        print(f"Error loading existing data from GCS: {e}. Starting with empty data.")
        existing_data = []

    new_scraped_data = []
    
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("headless")
    options.add_argument("--no-sandbox") # Docker 환경에서 필요할 수 있음
    options.add_argument("--disable-dev-shm-usage") # Docker 환경에서 필요할 수 있음
    
    try:
        browser = webdriver.Chrome(options=options)

        url = 'https://www.algumon.com'
        browser.get(url)

        titleselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/div/p[2]/span/a"
        priceselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/p[1]"
        linkselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/div/p[2]/span/a"
        timestampselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/p[2]/small"
        
        last_count = 0

        for _ in range(2):
            time.sleep(5) # 페이지 로딩 대기

            current_titles = browser.find_elements(By.XPATH, titleselector)
            current_prices = browser.find_elements(By.XPATH, priceselector)
            current_links = browser.find_elements(By.XPATH, linkselector)
            current_timestamps = browser.find_elements(By.XPATH, timestampselector)

            for i in range(last_count, len(current_titles)):
                timestamp_text = current_timestamps[i].text
                parsed_datetime = parse_timestamp(timestamp_text)

                if parsed_datetime:
                    formatted_timestamp = parsed_datetime.strftime("%Y/%m/%d-%H:%M")
                else:
                    formatted_timestamp = timestamp_text  # 파싱 실패 시 원래 텍스트 유지

                product_info = {
                    'title': current_titles[i].text,
                    'price': current_prices[i].text,
                    'link': current_links[i].get_attribute("href"),
                    'timestamp': formatted_timestamp
                }
                
                if not any(item['title'] == product_info['title'] and item['link'] == product_info['link'] for item in existing_data + new_scraped_data):
                    new_scraped_data.append(product_info)
                # 기존 데이터 및 이번에 스크래핑한 데이터 내에서 중복 확인
#                for item in existing_data + new_scraped_data:
#                    if item['title'] == product_info['title'] and item['link'] == product_info['link']:
#                        is_duplicate = True
#                        continue
#                
#                    if not is_duplicate:
#                        new_scraped_data.append(product_info)
            
            last_count = len(current_titles)
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    except Exception as e:
        print(f"Error during web scraping: {e}")
    finally:
        if 'browser' in locals() and browser:
            browser.quit()

    # 기존 데이터와 새로 스크래핑한 데이터 병합
    combined_data = existing_data + new_scraped_data
    
    # 'no' 필드 재정렬 및 번호 부여
    for i, item in enumerate(combined_data):
        item['no'] = i + 1

    # 업데이트된 데이터를 JSON 형식으로 GCS에 업로드
    try:
        gcs_hook.upload(
            bucket_name=bucket_name,
            object_name=blob_name,
            data=json.dumps(combined_data, ensure_ascii=False, indent=4),
            mime_type='application/json'
        )
        print(f"Successfully uploaded {len(combined_data)} items to gs://{bucket_name}/{blob_name}")
    except Exception as e:
        print(f"Error uploading data to GCS: {e}")


with DAG(
    dag_id='hotdeal_scraper_to_gcs',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=timedelta(minutes=20), # 예: 매시간 실행
    catchup=False,
    tags=['web_scraping', 'gcs'],
) as dag:
    scrape_task = PythonOperator(
        task_id='scrape_and_upload_hotdeal_data',
        python_callable=scrape_and_process_data,
    )