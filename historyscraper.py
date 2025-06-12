from playwright.sync_api import sync_playwright
import time
import json
import random
from datetime import datetime, timedelta
import re

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

def scrape_data_playwright_optimized(**kwargs):
    batch_size = 10000  # 한 번에 처리하고 저장할 데이터 개수
    data_batch = []
    base_url = 'https://www.algumon.com'
    now = datetime.now() # 현재 시간 확보

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        page.goto(base_url)

        titleselector = "xpath=/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/div/p[2]/span/a"
        priceselector = "xpath=/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/p[1]"
        linkselector = "xpath=/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/div/p[2]/span/a"
        timestampselector = "xpath=/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/p[2]/small"
        last_count = 0
        scroll_count = 0
        max_scrolls = 77

        try:
            while scroll_count < max_scrolls:
                time.sleep(random.uniform(7, 10))

                current_titles = page.locator(titleselector).all()
                current_prices = page.locator(priceselector).all()
                current_links = page.locator(linkselector).all()
                current_timestamps = page.locator(timestampselector).all()

                for i in range(last_count, len(current_titles)):
                    relative_link = current_links[i].get_attribute("href")
                    full_link = base_url + relative_link
                    timestamp_text = current_timestamps[i].inner_text()
                    parsed_datetime = parse_timestamp(timestamp_text)

                    if parsed_datetime:
                        formatted_timestamp = parsed_datetime.strftime("%Y/%m/%d-%H:%M")
                    else:
                        formatted_timestamp = timestamp_text  # 파싱 실패 시 원래 텍스트 유지

                    product_info = {
                        'no': i + 1,
                        'title': current_titles[i].inner_text(),
                        'price': current_prices[i].inner_text(),
                        'link': full_link,
                        'timestamp': formatted_timestamp
                    }
                    data_batch.append(product_info)

                    if len(data_batch) >= batch_size:
                        with open("history2.json", "a", encoding="utf-8") as f:
                            json.dump(data_batch, f, indent=4, ensure_ascii=False)
                            f.write('\n')
                        data_batch = []

                last_count = len(current_titles)
                page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                scroll_count += 1

            # 마지막 남은 데이터 배치 처리
            if data_batch:
                with open("add.json", "a", encoding="utf-8") as f:
                    json.dump(data_batch, f, indent=4, ensure_ascii=False)
                    f.write('\n')

        finally:
            browser.close()

scrape_data_playwright_optimized()