import requests
from bs4 import BeautifulSoup

url = "https://mq.lordfilm17.ru/filmy/42185-djuna-2021-8816-63057.html"

try:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    response = requests.get(url, headers=headers)
    print(response.text)
    response.raise_for_status()  # Проверка на ошибки HTTP

    soup = BeautifulSoup(response.text, 'html.parser')

    # print(soup)
    # Лайки (класс psc)
    likes_element = soup.find('span', class_='psc')
    likes = likes_element.text.strip() if likes_element else "Не найдено"

    # Дизлайки (класс msc)
    dislikes_element = soup.find('span', class_='msc')
    dislikes = dislikes_element.text.strip() if dislikes_element else "Не найдено"

    # Рейтинг (из slide-circle)
    rating_div = soup.find('div', class_='slide-circle')
    rating = "Не найдено"

    if rating_div:
        # Ищем все внутренние div внутри slide-circle
        inner_divs = rating_div.find_all('div', recursive=False)

        if len(inner_divs) >= 1:
            # Берем текст первого div (например, "7.6рейтинг") и разбиваем
            rating_text = inner_divs[0].get_text(strip=True)
            # Извлекаем только число (7.6)
            rating = rating_text.split()[0] if rating_text else "Не найдено"

    print(f"Лайки: {likes}")
    print(f"Дизлайки: {dislikes}")
    print(f"Рейтинг: {rating}")

except requests.exceptions.RequestException as e:
    print(f"Ошибка при запросе: {e}")
except Exception as e:
    print(f"Произошла ошибка: {e}")