import requests
from bs4 import BeautifulSoup
import csv
import random
import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import json
import sqlite3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

url = "http://books.toscrape.com/"

MAX_RETRIES = 3
DELAY_RANGE = (1, 3)  # задержка между попытками (сек)


def fetch_page(url):
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {"User-Agent": random.choice(USER_AGENTS)}
        try:
            logging.info(f"Запрос к {url} (попытка {attempt})")
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                logging.warning(f"Статус-код: {response.status_code}")
        except requests.RequestException as e:
            logging.error(f"Ошибка запроса: {e}")
        if attempt < MAX_RETRIES:
            delay = random.uniform(*DELAY_RANGE)
            logging.info(f"Задержка {delay:.1f} сек перед следующей попыткой...")
            time.sleep(delay)
    logging.error("Не удалось получить страницу после нескольких попыток.")
    return None

def save_to_json(results, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logging.info(f"Сохранено {len(results)} книг в {filename}")

def save_to_sqlite(results, db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS books (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            price TEXT
        )
    """)
    c.execute("DELETE FROM books")  # очищаем таблицу перед записью
    for book in results:
        c.execute("INSERT INTO books (title, price) VALUES (?, ?)", (book["Название"], book["Цена"]))
    conn.commit()
    conn.close()
    logging.info(f"Сохранено {len(results)} книг в {db_name}")

def main():
    logging.info("Запуск парсера...")
    html = fetch_page(url)
    if not html:
        logging.error("Парсинг прерван: нет HTML.")
        return
    soup = BeautifulSoup(html, "lxml")
    books = soup.select(".product_pod")
    results = []
    for book in books:
        title_tag = book.h3.a if book.h3 and book.h3.a else None
        price_tag = book.select_one(".product_price .price_color")
        title = title_tag["title"].strip() if title_tag and "title" in title_tag.attrs else "(нет названия)"
        price = "(нет цены)"
        if price_tag:
            price_text = price_tag.get_text()
            if isinstance(price_text, list):
                price_text = price_text[0] if price_text else ""
            price = str(price_text).strip()
        price_clean = price.replace("£", "").strip()
        results.append({"Название": title, "Цена": price_clean})
    with open("books.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Название", "Цена"])
        writer.writeheader()
        writer.writerows(results)
    logging.info(f"Сохранено {len(results)} книг в books.csv")
    save_to_json(results, "books.json")
    save_to_sqlite(results, "books.db")

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(main, 'interval', hours=1)
    scheduler.start()
    logging.info("Планировщик запущен. Парсер будет запускаться каждый час.")
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Планировщик остановлен.")

if __name__ == "__main__":
    start_scheduler() 