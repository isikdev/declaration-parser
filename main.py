import requests
import re
import logging
import pandas as pd
import random
import json
import time
from tqdm import tqdm
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from bs4 import BeautifulSoup
import jwt 

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('scraper.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

field_mapping = {
    "id": "ID",
    "applicantName": "Название Заявителя",
    "applicantInn": "ИНН Заявителя",
    "manufacterName": "Название Производителя",
    "manufacterInn": "ИНН Производителя",
    "declDate": "Дата Регистрации Декларации",
    "applicantPhone": "Телефон Заявителя",
    "applicantEmail": "Электронная Почта Заявителя",
}

status_mapping = {
    "Черновик": 1,
    "Действует": 2,
    "Прекращён": 3,
    "Приостановлен": 4,
    "Возобновлён": 5,
    "Архивный": 6,
    "Направлено уведомление о прекращении": 7,
    "Выдано предписание": 8,
    "Ожидает проверки оператора реестра": 9,
    "Недействителен": 10
}

def validate_date(date_str):
    try:
        return datetime.strptime(date_str, "%d-%m-%Y")
    except ValueError:
        return None

def generate_uuid():
    return random.randint(1000, 9999)

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.exceptions.RequestException,))
)
def fetch_page_retry(session, url, payload):
    logger.info(f"Отправка запроса: {url} с payload: {payload}")
    resp = session.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding if resp.apparent_encoding else 'utf-8'
    try:
        data = resp.json()
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка декодирования JSON: {e}")
        return [], 0
    items = data.get("items", [])
    total = data.get("total", 0)
    logger.info(f"Получено {len(items)} элементов, всего {total} для запроса.")
    return items, total

def generate_date_ranges(start_date, end_date):

    start = datetime.strptime(start_date, "%d-%m-%Y")
    end = datetime.strptime(end_date, "%d-%m-%Y")
    ranges = []
    current = start
    while current <= end:
        ranges.append((current.strftime("%Y-%m-%d"), current.strftime("%Y-%m-%d")))
        current += timedelta(days=1)
    logger.info(f"Сгенерировано {len(ranges)} диапазонов дат.")
    return ranges

def process_date_range(session, url, dr, statuses, decl_types, decl_app_types):

    payload = {
        "page": 0,
        "size": 1000,
        "columnsSort": [{"column": "declDate", "sort": "DESC"}],
        "filter": {
            "status": statuses,
            "idDeclType": decl_types,
            "idApplicantType": decl_app_types,
            "idCertObjectType": [],
            "idDeclScheme": [],
            "idGroupRU": [],
            "idGroupEEU": [],
            "idProductEEU": [],
            "idProductOrigin": [],
            "idProductRU": [],
            "idProductType": [],
            "idTechReg": [],
            "isProtocolInvalid": None,
            "number": None,
            "regDate": {
                "minDate": dr[0],
                "maxDate": dr[1]
            },
            "endDate": {
                "minDate": None,
                "maxDate": None
            },
            "awaitOperatorCheck": None,
            "editApp": None,
            "violationSendDate": None,
            "checkerAIResult": None,
            "checkerAIProtocolsResults": None,
            "checkerAIProtocolsMistakes": None,
            "hiddenFromOpen": None,
            "columnsSearch": []
        }
    }
    all_items = []
    try:
        logger.info(f"Обработка диапазона {dr[0]} - {dr[1]} (страница 0)")
        items, total = fetch_page_retry(session, url, payload)
        all_items.extend(items)
        if total > 1000:
            pages = (total + 999) // 1000
            for page_num in range(1, pages):
                payload["page"] = page_num
                logger.info(f"Обработка диапазона {dr[0]} - {dr[1]} (страница {page_num})")
                items, _ = fetch_page_retry(session, url, payload)
                all_items.extend(items)
        logger.info(f"Диапазон {dr[0]} - {dr[1]}: получено {len(all_items)} деклараций.")
    except Exception as e:
        logger.error(f"Ошибка при обработке диапазона {dr}: {e}", exc_info=True)
    return all_items

def get_all_declarations(token, start_date, end_date, statuses, decl_types, decl_app_types, cookies, proxy, max_workers=10):

    url = "https://pub.fsa.gov.ru/api/v1/rds/common/declarations/get"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://pub.fsa.gov.ru",
        "Referer": "https://pub.fsa.gov.ru/rds/declaration",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }
    safe_cookies = {}
    for k, v in cookies.items():
        if all(ord(c) < 128 for c in k) and all(ord(c) < 128 for c in v):
            safe_cookies[k] = v
        else:
            logger.info(f"Удаляем потенциально проблемный cookie: {k} = {v}")
    proxies = {
        'http': f'socks5://{proxy}',
        'https': f'socks5://{proxy}'
    }
    session = requests.Session()
    session.headers.update(headers)
    session.cookies.update(safe_cookies)
    session.proxies.update(proxies)
    adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    date_ranges = generate_date_ranges(start_date, end_date)
    all_declarations = []
    logger.info("Начало параллельного парсинга диапазонов дат.")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_date_range, session, url, dr, statuses, decl_types, decl_app_types)
            for dr in date_ranges
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Парсинг диапазонов"):
            try:
                res = future.result()
                if res:
                    all_declarations.extend(res)
            except Exception as e:
                logger.error(f"Ошибка при выполнении future: {e}", exc_info=True)
    logger.info(f"Всего загружено деклараций: {len(all_declarations)}")
    return all_declarations, session

def fetch_applicant_contacts_selenium(doc_id, proxy=None, max_attempts=3):

    url = f"https://pub.fsa.gov.ru/rds/declaration/view/{doc_id}/applicant"
    logger.info(f"Selenium: Запуск браузера для id={doc_id} по URL: {url}")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.page_load_strategy = "normal"
    if proxy:
        options.add_argument(f"--proxy-server=socks5://{proxy}")
        logger.info(f"Selenium: Используем прокси: socks5://{proxy}")

    try:
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        logger.error(f"Selenium: Ошибка запуска Chrome для id={doc_id}: {e}")
        return None, None

    driver.set_page_load_timeout(60)
    attempt = 0
    rendered_html = None
    while attempt < max_attempts:
        try:
            logger.info(f"Selenium: Попытка {attempt+1} загрузить страницу для id={doc_id}")
            driver.get(url)
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Контактные данные')]"))
            )
            rendered_html = driver.page_source
            logger.info(f"Selenium: Страница успешно загружена для id={doc_id}")
            break
        except (TimeoutException, WebDriverException) as e:
            logger.error(f"Selenium: Ошибка загрузки страницы для id={doc_id} на попытке {attempt+1}: {e}")
            attempt += 1
            time.sleep(2)
            if attempt == max_attempts:
                logger.error(f"Selenium: Превышено число попыток для id={doc_id}.")
                driver.quit()
                return None, None
            try:
                driver.refresh()
            except Exception:
                pass
    driver.quit()

    soup = BeautifulSoup(rendered_html, "html.parser")
    phone = None
    email = None

    container = soup.find("fgis-rds-view-contacts")
    if container:
        logger.info(f"Selenium: Найден контейнер <fgis-rds-view-contacts> для id={doc_id}")
        edit_containers = container.find_all("fgis-card-edit-row-two-columns")
        logger.info(f"Selenium: Найдено {len(edit_containers)} блоков <fgis-card-edit-row-two-columns> для id={doc_id}")
        for ec in edit_containers:
            rows = ec.find_all("fgis-card-info-row")
            for row in rows:
                header_div = row.find("div", class_="info-row__header")
                text_div = row.find("div", class_="info-row__text")
                if header_div and text_div:
                    header_text = header_div.get_text(strip=True)
                    if "Номер телефона" in header_text and not phone:
                        p = text_div.find("p")
                        if p:
                            phone = p.get_text(strip=True)
                            logger.info(f"Selenium: Найден телефон для id={doc_id}: {phone}")
                    elif "Адрес электронной почты" in header_text and not email:
                        p = text_div.find("p")
                        if p:
                            email = p.get_text(strip=True)
                            logger.info(f"Selenium: Найдена почта для id={doc_id}: {email}")
    else:
        logger.warning(f"Selenium: Контейнер <fgis-rds-view-contacts> не найден для id={doc_id}")

    if not phone:
        logger.info(f"Selenium: Пробуем найти телефон через regex для id={doc_id}")
        phone_pattern = re.compile(r'\+7\s*\(?\d{3}\)?[\s-]?\d{3}[\s-]?\d{4}')
        phones_found = phone_pattern.findall(rendered_html)
        if phones_found:
            phone = phones_found[0]
            logger.info(f"Selenium: Найден телефон (regex) для id={doc_id}: {phone}")
    if not email:
        logger.info(f"Selenium: Пробуем найти e-mail через regex для id={doc_id}")
        email_pattern = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+')
        emails_found = email_pattern.findall(rendered_html)
        if emails_found:
            email = emails_found[0]
            logger.info(f"Selenium: Найден e-mail (regex) для id={doc_id}: {email}")

    if not phone:
        logger.warning(f"Selenium: Телефон не найден для id={doc_id}")
    if not email:
        logger.warning(f"Selenium: Почта не найдена для id={doc_id}")
    return phone, email

def fetch_applicant_contacts(doc_id, session, proxy):

    return fetch_applicant_contacts_selenium(doc_id, proxy=proxy)

def enrich_with_contacts(all_declarations, session, proxy, max_workers=10):

    def enrich_declaration(item):
        doc_id = item.get("id")
        if not doc_id:
            logger.warning("Декларация без id — пропускаем получение контактов.")
            return item
        logger.info(f"Начало получения контактов для id={doc_id}")
        try:
            phone, email = fetch_applicant_contacts(doc_id, session, proxy)
            item["applicantPhone"] = phone if phone else ""
            item["applicantEmail"] = email if email else ""
            logger.info(f"Завершено получение контактов для id={doc_id}")
        except Exception as e:
            logger.error(f"Ошибка при обработке id={doc_id}: {e}", exc_info=True)
            item["applicantPhone"] = ""
            item["applicantEmail"] = ""
        return item

    logger.info("Запуск параллельного обогащения деклараций контактными данными.")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        enriched_declarations = list(
            tqdm(executor.map(enrich_declaration, all_declarations),
                 total=len(all_declarations),
                 desc="Доп. парсинг телефон/почта")
        )
    logger.info("Завершено обогащение деклараций контактными данными.")
    return enriched_declarations

def clean_illegal_chars(df):

    def clean_text(x):
        if isinstance(x, str):
            return re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', x)
        return x
    return df.apply(lambda col: col.map(clean_text) if col.dtype == object else col)

def convert_to_excel(declarations, output_file):

    if not declarations:
        print("Нет данных для сохранения.")
        return
    df = pd.DataFrame(declarations)
    if "declDate" in df.columns:
        df["declDate"] = pd.to_datetime(df["declDate"], errors="coerce").dt.strftime("%d-%m-%Y")
        df = df.sort_values(by="declDate", ascending=True)
    df.rename(columns=field_mapping, inplace=True, errors="ignore")
    df["Время Парсинга"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
    if "ID" in df.columns:
        df["Ссылка на Документ"] = df["ID"].apply(
            lambda x: f"https://pub.fsa.gov.ru/rds/declaration/view/{x}/common" if pd.notnull(x) else ""
        )
    df = clean_illegal_chars(df)
    try:
        df.to_excel(output_file, index=False)
        print(f"Данные сохранены в {output_file}")
        logger.info(f"Данные сохранены в {output_file}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении в Excel: {e}", exc_info=True)
        print(f"Ошибка при сохранении в Excel: {e}")

def save_results_txt(results, filename="contacts.txt"):
    with open(filename, "w", encoding="utf-8") as f:
        for doc_id, phone, email in results:
            f.write(f"ID {doc_id}: телефон = {phone}, email = {email}\n")
    logger.info(f"Результаты сохранены в {filename}")

def is_token_valid(token):

    try:
        decoded = jwt.decode(token, options={"verify_signature": False})
        exp = decoded.get("exp")
        if exp:
            return datetime.fromtimestamp(exp) > datetime.now()
        return False
    except Exception as e:
        logger.error(f"Ошибка при декодировании токена: {e}")
        return False

if __name__ == "__main__":
    print("=== Парсер деклараций FSA ===")
    logger.info("Запуск скрипта.")
    
    token = "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzM4NjEyOTM1LCJpYXQiOjE3Mzg1ODQxMzV9.TCi_LfWZ42sHfE9nfuezAaoBv2p0ARUVE8BY_bUErTYzjk-fiCQcoW65S5Kyuzrrb9veT_H49A7g8sZv0iSCTyugoT-m5BAJMs1YmxpoPKdTDh7H7jlNmjlrJs-klgBP5OCbVKG-kkjnnzZ6G_8UuPdxmN4Kt0TY6nIrSxpo5DtujfFjhQKD1F4wBXjTF8SJeGtzgOARgNjRqL_CT2eCdrqWjxvy3QmDUp0gPnNpVJtJ2BLNmvlZTDlIjEVFC-PFB-susQ-OSGIYrQgCB7Sc3exTFUUxXN2aq0poGgvx1urwGFegZgSqmnuRXair-DAEN9C9gPKGBXUXxxN75LNBPEW3e7EzS2hh8NnlLrLRQjYZbjMOrjmd1B2PAW0Dj7vbJzD_k6087-ms62p0VguqbclyaBQvkl8Kw-__uJzPU6UnRhaA9brJ7mLN2CFoyjtgfCntfeOPVxUOm2kfqEcN1K0UhGIp1c9UjHI-41sRduiQxCmadybj6jV3cry1IWUMN4RqT6lrLwsPznfAKdDn_eP3q4dyBiOUTZYtNQRRPCNRRBiYEoabVxKEjKM2KjZFLKwIQPXp3MzC1NJrQAb6UdxgNTR9dBoeCeB0HF0IyC2uat-3jHjVf8WfyzaYLeRy0c-KOgP8BwN_8nfsE8Jn4W0M4Hazvom2E21py6NXpjY"  # Обновите токен
    if not is_token_valid(token):
        print("Токен истёк. Пожалуйста, обновите его.")
        logger.error("Токен истёк.")
        exit(1)
    
    cookies = {
        "fgis_token": "eyJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzM4NjEyOTM1LCJpYXQiOjE3Mzg1ODQxMzV9.TCi_LfWZ42sHfE9nfuezAaoBv2p0ARUVE8BY_bUErTYzjk-fiCQcoW65S5Kyuzrrb9veT_H49A7g8sZv0iSCTyugoT-m5BAJMs1YmxpoPKdTDh7H7jlNmjlrJs-klgBP5OCbVKG-kkjnnzZ6G_8UuPdxmN4Kt0TY6nIrSxpo5DtujfFjhQKD1F4wBXjTF8SJeGtzgOARgNjRqL_CT2eCdrqWjxvy3QmDUp0gPnNpVJtJ2BLNmvlZTDlIjEVFC-PFB-susQ-OSGIYrQgCB7Sc3exTFUUxXN2aq0poGgvx1urwGFegZgSqmnuRXair-DAEN9C9gPKGBXUXxxN75LNBPEW3e7EzS2hh8NnlLrLRQjYZbjMOrjmd1B2PAW0Dj7vbJzD_k6087-ms62p0VguqbclyaBQvkl8Kw-__uJzPU6UnRhaA9brJ7mLN2CFoyjtgfCntfeOPVxUOm2kfqEcN1K0UhGIp1c9UjHI-41sRduiQxCmadybj6jV3cry1IWUMN4RqT6lrLwsPznfAKdDn_eP3q4dyBiOUTZYtNQRRPCNRRBiYEoabVxKEjKM2KjZFLKwIQPXp3MzC1NJrQAb6UdxgNTR9dBoeCeB0HF0IyC2uat-3jHjVf8WfyzaYLeRy0c-KOgP8BwN_8nfsE8Jn4W0M4Hazvom2E21py6NXpjY",
        "language": "ru",
        "is_esia_auth": "false",
        "yandexuid": "9603053731735910423",
        "yuidss": "9603053731735910423",
        "_ym_uid": "1738243086981671201",
        "_ym_d": "1738243086",
        "_ym_isad": "1",
    }
    proxy = "31.128.40.174:1080"  # SOCKS5-прокси
    
    while True:
        start_date_input = input("Введите дату начала (dd-mm-yyyy): ").strip()
        start_date_val = validate_date(start_date_input)
        if start_date_val:
            break
        print("Неверный формат даты. Пример: 01-01-2025")
    
    while True:
        end_date_input = input("Введите дату окончания (dd-mm-yyyy): ").strip()
        end_date_val = validate_date(end_date_input)
        if end_date_val:
            break
        print("Неверный формат даты. Пример: 10-01-2025")
    
    if start_date_val > end_date_val:
        print("Дата окончания не может быть раньше даты начала. Завершение.")
        logger.error("Дата окончания не может быть раньше даты начала.")
        exit(1)
    
    statuses = [
        status_mapping["Черновик"],
        status_mapping["Действует"],
        status_mapping["Прекращён"],
        status_mapping["Приостановлен"],
        status_mapping["Возобновлён"],
        status_mapping["Архивный"],
        status_mapping["Направлено уведомление о прекращении"],
        status_mapping["Выдано предписание"],
        status_mapping["Ожидает проверки оператора реестра"],
        status_mapping["Недействителен"]
    ]
    declaration_types = [1, 2, 3, 4, 5]
    applicant_types = [1, 2, 3, 4, 5]
    
    logger.info(f"Парсинг деклараций с {start_date_input} по {end_date_input}")
    all_declarations, session = get_all_declarations(
        token=token,
        start_date=start_date_input,
        end_date=end_date_input,
        statuses=statuses,
        decl_types=declaration_types,
        decl_app_types=applicant_types,
        cookies=cookies,
        proxy=proxy,
        max_workers=10
    )
    
    print(f"Всего деклараций: {len(all_declarations)}")
    if all_declarations:
        all_declarations = enrich_with_contacts(all_declarations, session, proxy, max_workers=5)
    
    print("\nРезультаты парсинга контактов:")
    contacts_results = []
    for item in all_declarations:
        doc_id = item.get("id")
        phone = item.get("applicantPhone")
        email = item.get("applicantEmail")
        print(f"ID {doc_id}: телефон = {phone}, email = {email}")
        contacts_results.append((doc_id, phone, email))
    
    save_results_txt(contacts_results, filename="contacts.txt")
    
    if all_declarations:
        user_filename = "result_parsing"
        random_id = generate_uuid()
        output_file = f"{user_filename}_{start_date_val.year}_{start_date_val.month:02d}_{random_id}.xlsx"
        convert_to_excel(all_declarations, output_file)
    else:
        print("Нет данных для сохранения в Excel.")
    
    logger.info("Парсинг завершён.")
    print("Парсинг завершён.")
