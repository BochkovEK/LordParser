from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

url = "https://mq.lordfilm17.ru/filmy/42185-djuna-2021-8816-63057.html"

driver = webdriver.Chrome(service=Service(executable_path="chromedriver.exe"))  # или ChromeDriverManager
driver.get(url)

try:
    # Ждем загрузки общего числа голосов
    total_votes = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "vote-num-id-42185"))
    ).text.strip()

    # Ждем загрузки рейтинга
    rating_div = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "slide-circle"))
    )
    rating = rating_div.find_element(By.XPATH, ".//div[not(*)]").text.split()[0]

    print(f"Общее количество голосов: {total_votes}")
    print(f"Рейтинг: {rating}")

finally:
    driver.quit()