import re
import spacy
import json
import time
import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium import webdriver
from threading import Lock

# Global result list and lock for thread-safe writes
all_data = []
lock = Lock()

# Load the spacy English model
nlp = spacy.load("en_core_web_sm")

def print_exception_details(e):
    exc_type, exc_obj, exc_tb = sys.exc_info()
    Exception_Type = exc_type.__name__
    Line_No = exc_tb.tb_lineno
    Error_Message = str(e)
    if '(Session info:' in Error_Message:
        Error_Message = Error_Message.partition('(Session info:')[0].strip()
    Error_Message = Error_Message.replace('\n', ', ')
    Function_name = exc_tb.tb_frame.f_code.co_name
    File_Name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    Timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    Error_Final = (
        f"[{Timestamp}] ERROR in {Function_name} ({File_Name}:{Line_No}) - "
        f"{Exception_Type}: {Error_Message}"
    )
    print(Error_Final)

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--window-size=1920,1080')
    service = Service('C:\\chromedriver.exe')
    return webdriver.Chrome(service=service, options=chrome_options)

def remove_html_string(text):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', text)
    cleantext = re.sub('\s+', ' ', cleantext)
    return cleantext

def get_json_data():
    with open("data.json", 'r', encoding="utf-8") as f:
        return json.load(f)

def scrap(driver, data):
    try:
        # print(f"Scraping: {data['url']}")
        driver.get(data['url'])
        time.sleep(3)

        newjson = {
            "name": data['name'],
            "designation": data['designation'],
            "topics": data['topics'],
            "sdgs": data['sdgs'],
            "url": data['url']
        }

        element = driver.find_element(By.XPATH, '//*[@id="block-ke-subtheme-content"]/div/article/div/div[1]/div[2]/div[2]/div/div/div[2]')
        speaker_detail = element.text

        if speaker_detail:
            speaker_detail = remove_html_string(speaker_detail)
            nlp_tags = nlp(speaker_detail)

            all_GPE = [ent.text for ent in nlp_tags.ents if ent.label_ == "GPE"]
            all_ORG = [ent.text for ent in nlp_tags.ents if ent.label_ == "ORG"]

            newjson['gpes'] = ",".join(all_GPE)
            newjson['orgs'] = ",".join(all_ORG)

        with lock:
            all_data.append(newjson)
            print("juned length is ",len(all_data))

    except Exception as e:
        print_exception_details(e)

def worker_thread(datas, driver):
    for data in datas:
        scrap(driver, data)

def main():
    all_input_data = get_json_data()
    num_workers = 5
    chunk_size = len(all_input_data) // num_workers + 1

    # Split data into chunks
    chunks = [all_input_data[i:i + chunk_size] for i in range(0, len(all_input_data), chunk_size)]

    # Create and manage browser instances
    drivers = [init_driver() for _ in range(num_workers)]

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(len(chunks)):
            futures.append(executor.submit(worker_thread, chunks[i], drivers[i]))

        for future in as_completed(futures):
            future.result()

    for driver in drivers:
        driver.quit()

    # Save results
    with open("new_data.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=4, ensure_ascii=False)

    print("Scraping completed. Data saved to new_data.json")

if __name__ == "__main__":
    main()
