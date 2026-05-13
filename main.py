import requests
import json
import boto3
import os
from datetime import datetime

WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
VK_ACCESS_KEY = os.getenv("VK_ACCESS_KEY")
VK_SECRET_KEY = os.getenv("VK_SECRET_KEY")
BUCKET_NAME = "weather-task-data"

CITIES = ["Moscow", "Saint-Petersburg", "Novosibirsk", "Yekaterinburg", "Vladivostok"]

def get_weather():
    temperatures = []
    print("1. Собираю данные о погоде через WeatherAPI...")

    for city in CITIES:
        url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={city}&aqi=no"
        try:
            res = requests.get(url)
            if res.status_code == 200:
                temp = res.json()['current']['temp_c']
                print(f"   - {city}: {temp}°C")
                temperatures.append(temp)
            else:
                print(f"   - Ошибка для {city}: статус {res.status_code}")
        except Exception as e:
            print(f"   - Ошибка в {city}: {e}")

    if not temperatures:
        return None

    return round(sum(temperatures) / len(temperatures), 2)


def upload_to_vk_s3(file_name):
    print(f"2. Отправляю файл {file_name} в VK Cloud S3...")
    try:
        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://hb.bizmrg.com',
            aws_access_key_id=VK_ACCESS_KEY,
            aws_secret_access_key=VK_SECRET_KEY
        )

        s3.upload_file(
            file_name,
            BUCKET_NAME,
            file_name,
            ExtraArgs={'ACL': 'public-read'}
        )

        print("   ✅ Успешно загружено в облако!")
        url = f"https://hb.bizmrg.com/{BUCKET_NAME}/{file_name}"
        print(f"   🔗 Прямая ссылка на результат: {url}")

    except Exception as e:
        print(f"   ❌ Ошибка при загрузке в S3: {e}")


if __name__ == "__main__":
    if not WEATHER_API_KEY or not VK_ACCESS_KEY or not VK_SECRET_KEY:
        print("❌ Ошибка: Ключи доступа не найдены в переменных окружения!")
    else:
        avg = get_weather()
        if avg is not None:
            print("-" * 30)
            print(f"СРЕДНЯЯ ТЕМПЕРАТУРА ПО РОССИИ: {avg}°C")

            result_data = {
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "average_temperature": avg,
                "cities_count": len(CITIES),
                "source": "WeatherAPI"
            }

            file_name = "result.json"
            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(result_data, f, indent=4)

            upload_to_vk_s3(file_name)
        else:
            print("❌ Не удалось получить данные о погоде.")
