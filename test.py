from selenium import webdriver
from selenium.webdriver.common.by import By
import time

titles, prices, categories, links, timestamps, = [], [], [], [], []

def func():
    
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("headless")
    browser = webdriver.Chrome(options=options)
    
    url = 'https://www.algumon.com'
    browser.get(url)
    
    actions = browser.find_element(By.CSS_SELECTOR , 'body')
    titleselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/div/p[2]/span/a"
    priceselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/p[1]"
    linkselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/div/p[2]/span/a"
    timestampselector = "/html/body/div[6]/div[2]/ul/li/div[1]/div[2]/p[2]/small"
    last_count = 0
    
    for _ in range(3):
    
        time.sleep(3)
        
        # 현재 페이지의 모든 항목을 가져옴
        current_titles = browser.find_elements(By.XPATH, titleselector)
        current_prices = browser.find_elements(By.XPATH, priceselector)
        current_links = browser.find_elements(By.XPATH, linkselector)
        current_timestamps = browser.find_elements(By.XPATH, timestampselector)
        
        for i in range(last_count, len(current_titles)):
            titles.append(current_titles[i].text)
            prices.append(current_prices[i].text)
            links.append(current_links[i].get_attribute("href"))
            timestamps.append(current_timestamps[i].text)
            categories.append('')
        # 이번에 처리한 항목의 수를 업데이트
        last_count = len(current_titles)

        # 페이지 끝으로 스크롤
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    browser.quit()
        
    return titles, prices, categories ,links, timestamps

func()
print(len(titles))