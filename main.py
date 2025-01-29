from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading

def get_authorization_token():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Запуск в фоновом режиме
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')

    # Автоматически управляем ChromeDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        # Откройте страницу
        driver.get("https://pub.fsa.gov.ru/rds/declaration")
        time.sleep(5)  # Подождите, пока страница полностью загрузится

        # Если требуется логин, выполните его здесь
        # Пример (замените селекторы и данные на реальные):
        # username_input = driver.find_element(By.ID, "username")
        # password_input = driver.find_element(By.ID, "password")
        # login_button = driver.find_element(By.ID, "login-button")
        # username_input.send_keys("your_username")
        # password_input.send_keys("your_password")
        # login_button.click()
        # time.sleep(5)  # Подождите, пока вход завершится

        # Извлеките токен из Local Storage или из куки
        # Пример извлечения из Local Storage:
        token = driver.execute_script("return window.localStorage.getItem('fgis_token');")
        print(f"Получен токен: {token}")
        return token
    finally:
        driver.quit()

def generate_links(declarations):
    base_url = "https://pub.fsa.gov.ru/rds/declaration/view/{}/common"
    links = []
    for decl in declarations:
        decl_id = decl.get("id")
        if decl_id:
            link = base_url.format(decl_id)
            links.append(link)
    return links

def save_link(link, filename, lock):
    with lock:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(link + "\n")

def fetch_page(session, url, payload, page, size, filename, lock):
    payload.update({"page": page, "size": size})
    try:
        response = session.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            for decl in items:
                decl_id = decl.get("id")
                if decl_id:
                    link = f"https://pub.fsa.gov.ru/rds/declaration/view/{decl_id}/common"
                    save_link(link, filename, lock)
            print(f"Страница {page} загружена, найдено {len(items)} деклараций.")
            return len(items)
        else:
            print(f"Не удалось получить страницу {page}: Статус код {response.status_code}")
            print(response.text)  # Вывод текста ответа для отладки
            return 0
    except Exception as e:
        print(f"Ошибка при загрузке страницы {page}: {e}")
        return 0

def get_all_declarations(token, start_date, end_date, filename="declaration_links.txt", max_workers=5):
    url = "https://pub.fsa.gov.ru/api/v1/rds/common/declarations/get"
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://pub.fsa.gov.ru",
        "Referer": "https://pub.fsa.gov.ru/rds/declaration",
        "Sec-CH-UA": '"Not A(Brand";v="8", "Chromium";v="132", "Google Chrome";v="132"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    
    cookies = {
        "JSESSIONID": "42F2D63984E68DD5E64076AFE429ACA3",
        "_ym_d": "1738069299",
        "_ym_isad": "1",
        "_ym_uid": "1738069299721366238",
    }
    
    session = requests.Session()
    session.headers.update(headers)
    session.cookies.update(cookies)
    
    # Подготовка фильтров для даты регистрации декларации
    payload = {
        "page": 1,
        "size": 1000,
        "filters": {
            "regDate": {
                "startDate": f"{start_date}T00:00:00.000Z",
                "endDate": f"{end_date}T23:59:59.999Z"
            }
        }
    }
    
    # Первый запрос для получения общего количества элементов
    try:
        response = session.post(url, json=payload, timeout=30)
        if response.status_code != 200:
            print(f"Не удалось получить данные: Статус код {response.status_code}")
            print(response.text)
            return
        data = response.json()
        total = data.get("total", 0)
        size = data.get("size", 1000)
        print(f"Всего деклараций: {total}, размер страницы: {size}")
        
        if total == 0:
            print("Нет деклараций в указанном диапазоне дат.")
            return
        
        # Определение общего количества страниц
        total_pages = (total + size - 1) // size
        print(f"Всего страниц: {total_pages}")
        
        # Сохранение заголовков в файл (очистка предыдущего содержимого)
        open(filename, "w", encoding="utf-8").close()
        
        # Инициализация блокировки для записи в файл
        lock = threading.Lock()
        
        # Использование ThreadPoolExecutor для многопоточной загрузки
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for page in range(1, total_pages + 1):
                futures.append(executor.submit(fetch_page, session, url, {"filters": {"regDate": {"startDate": f"{start_date}T00:00:00.000Z", "endDate": f"{end_date}T23:59:59.999Z"}}}, page, size, filename, lock))
            
            # Отслеживание прогресса
            for future in as_completed(futures):
                pass  # Можно добавить дополнительную обработку здесь
        
        print(f"Все ссылки сохранены в {filename}")
    except Exception as e:
        print(f"Ошибка при начальном запросе: {e}")

def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    print("=== Парсер деклараций FSA ===")
    
    # Получение токена авторизации
    token = get_authorization_token()
    if not token:
        print("Не удалось получить токен авторизации. Завершение работы.")
        exit(1)
    
    # Ввод даты пользователем
    while True:
        start_date = input("Введите дату начала (YYYY-MM-DD): ").strip()
        if validate_date(start_date):
            break
        else:
            print("Неверный формат даты. Попробуйте снова.")
    
    while True:
        end_date = input("Введите дату окончания (YYYY-MM-DD): ").strip()
        if validate_date(end_date):
            break
        else:
            print("Неверный формат даты. Попробуйте снова.")
    
    # Проверка, что дата окончания не раньше даты начала
    if start_date > end_date:
        print("Дата окончания не может быть раньше даты начала. Завершение работы.")
        exit(1)
    
    # Запуск парсинга
    get_all_declarations(token, start_date, end_date)
