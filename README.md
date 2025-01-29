# Парсер Деклараций FSA

![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)

## О проекте

**Парсер Деклараций FSA** — это инструмент на Python для автоматического сбора ссылок на декларации с сайта [Федеральной службы по надзору в сфере защиты прав потребителей и благополучия человека (FSA)](https://pub.fsa.gov.ru/rds/declaration). Скрипт использует Selenium для взаимодействия с браузером, `webdriver-manager` для управления драйвером Chrome и `requests` для работы с API. Поддерживает многопоточность для ускорения процесса и позволяет пользователю задавать диапазон дат для поиска деклараций.

## Особенности

- **Выбор диапазона дат**: Пользователь может указать даты начала и окончания для фильтрации деклараций.
- **Многопоточность**: Использует несколько потоков для ускоренного сбора данных.
- **Генерация ссылок в реальном времени**: Ссылки сохраняются по мере их получения.
- **Отслеживание прогресса**: Показывает количество обработанных страниц и оставшихся деклараций.
- **Автоматическое управление ChromeDriver**: Использует `webdriver-manager` для установки и обновления драйвера.

## Установка

### Предварительные требования

- **Python 3.10+**
- **Google Chrome**: Убедитесь, что браузер установлен.

### Клонирование репозитория

```bash
git clone https://github.com/yourusername/fsa-declarations-scraper.git
cd fsa-declarations-scraper
