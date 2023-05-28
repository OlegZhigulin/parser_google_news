import ast
import random
import sqlite3
import time
from datetime import datetime
from multiprocessing import Pool

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common import exceptions

COUNT_USER_CREATE = 15
MAX_COUNT_PROCESS = 5
MAIN_URL = 'https://www.rbc.ru/'


def create_database():
    """ Создаем базу данных
    Создаем базу пользователей
    args: COUNT_USER_CREATE количество пользователей по дефолту 15
    """
    connect_sqlite = sqlite3.connect("Profile.db")
    cursor = connect_sqlite.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS CookieProfile
                    (id INTEGER PRIMARY KEY,
                    cookie TEXT,
                    date_create TEXT NOT NULL,
                    last_run_time TEXT,
                    count_uses INTEGER)
               """)
    connect_sqlite.commit()
    cursor.execute("SELECT count(id) FROM CookieProfile")
    flag = cursor.fetchone()[0]
    if not flag:
        for _ in range(COUNT_USER_CREATE):
            date_create = str(datetime.now())
            cursor.execute(
                "INSERT INTO CookieProfile (date_create, count_uses) VALUES (?, ?)",
                (date_create, 0)
            )
            connect_sqlite.commit()
            time.sleep(1.0)
    connect_sqlite.close()


def get_all_news():
    """Парсим сайт и получаем список всех новостей
    Returns:
        lists_news: список новостей
    """
    url = MAIN_URL
    headers = {
        "User-Agent": "Chrome/99.0.4758.87",
    }

    response = requests.get(url, headers=headers, timeout=30).text
    soup = BeautifulSoup(response, 'lxml')
    lists_news = soup.find_all(
        'a',
        class_='main__feed__link js-yandex-counter js-visited'
    )
    return lists_news


def get_all_news_link(lists_news):
    """Принимает список новостей и достаем урлы
    Args:
        lists_news: список новостей
    Returns:
        urls_lists: список урлов
    """
    urls_lists = [news.get('href', None) for news in lists_news]
    return urls_lists


def get_profiles():
    """Подключаемся к бд и собираем всех пользователей
    Returns:
        profiles: список пользователей
    """
    connect_sqlite = sqlite3.connect("Profile.db")
    cursor = connect_sqlite.cursor()
    cursor.execute("SELECT * FROM CookieProfile")
    profiles = cursor.fetchall()
    connect_sqlite.close()
    return profiles


def update_profile(profile, cookies):
    """Обновление куки пользователя, куки в бд хранятся строкой
    Args:
        profile: профиль пользователя
        cookies: новые куки
    """
    connect_sqlite = sqlite3.connect('Profile.db')
    cursor = connect_sqlite.cursor()
    cookie_str = str(cookies)
    last_run_time = str(datetime.now())
    count_uses = profile[4] + 1
    cursor.execute(
        "UPDATE CookieProfile SET cookie=?, last_run_time=?, count_uses=? WHERE id=?",
        (cookie_str, last_run_time, count_uses, profile[0])
    )
    connect_sqlite.commit()
    connect_sqlite.close()


def get_request(profile, url):
    """Здесь создается сессия, в нее добавляются куки при наличии
    рандомная задержка и переход на страницу с новостью
    пролистывание вниз и получение новых куки и обновление в бд
    сессия закрывается
    Args:
        profile: профиль пользователя
        url: рандомная ссылка из списка новостей
    """
    options = webdriver.ChromeOptions()
    options.add_argument('--ignore-certificate-errors-spki-list')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--ignore-ssl-errors')
    browser = webdriver.Chrome(chrome_options=options)
    browser.get(MAIN_URL)
    profile_cookie = profile[1]
    if profile_cookie is not None:
        lst_cook = ast.literal_eval(profile_cookie)
        for i in lst_cook:
            try:
                browser.add_cookie(i)
            except exceptions.InvalidCookieDomainException as error:
                print(error.msg)
    time.sleep(random.uniform(0.1, 10.0))
    browser.get(url)
    for _ in range(5):
        browser.execute_script('scrollBy(0, 300)')
        time.sleep(0.5)
    new_cookies = browser.get_cookies()
    update_profile(profile, new_cookies)
    browser.quit()


if __name__ == "__main__":
    create_database()
    lists_news = get_all_news()
    urls_lists = get_all_news_link(lists_news)
    profiles = get_profiles()
    data = []
    for profile in profiles:
        url = random.choice(urls_lists)
        tuple_of_profile_plus_url = (profile, url)
        data.append(tuple_of_profile_plus_url)
    with Pool(processes=MAX_COUNT_PROCESS) as pool:
        pool.starmap(get_request, data)
