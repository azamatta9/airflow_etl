from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import text




def extract_data():
    current_date = datetime.now().day
    count = 0
    main = pd.DataFrame(columns=['model', 'year', 'price', 'city', 'generation', 'kuzov', '\n'
                                'engine', 'probeg', 'transmission', 'privod', 'wheel', 'rastamozhka'])

    def get_card_url():
        for count in range(2, 3):
            url = f'https://kolesa.kz/cars/toyota/?page={count}'
            html = requests.get(url)
            print('Получили html страницы ждем 30 сек')
            sleep(30)
            all_data = BeautifulSoup(html.text, 'lxml')
            data = all_data.find_all('div', class_='a-list__item')
            for i in data:
                classes = i.get('class')
                if 'a-list__item--padding-vertical' not in classes:
                    date_of_card = i.find('span', class_="a-card__param a-card__param--date").text
                    if int(date_of_card.split(' ')[0]) == current_date:
                        card_url = 'https://kolesa.kz/' + i.find('a', class_= 'a-card__link').get('href')
                        print('получили url карты')
                        yield card_url

    for card_url in get_card_url():
        html = requests.get(card_url)
        print('из url карты получили html ждем 30 сек')
        sleep(2)
        data = BeautifulSoup(html.text, 'lxml')

        model = data.find('span', itemprop='brand').text.strip()
        year = data.find('span', class_='year').text.strip()
        price = data.find('div', class_='offer__price').text.strip()
        city = data.find('dd', class_='value').text.strip()
        generation = kuzov = engine = probeg = transmission = privod = wheel = rastamozhka = None
        generation1 = data.find('dt', class_='value-title', title='Поколение')
        if generation1:
            generation = generation1.find_next_sibling('dd').text.strip()
        kuzov1 = data.find('dt', class_='value-title', title='Кузов')
        if kuzov1:
            kuzov = kuzov1.find_next_sibling('dd').text.strip()
        engine1 = data.find('dt', class_='value-title', title='Объем двигателя, л')
        if engine1:
            engine = engine1.find_next_sibling('dd').text.strip()
        probeg1 = data.find('dt', class_='value-title', title='Пробег')
        if probeg1:
            probeg = probeg1.find_next_sibling('dd').text.strip()
        transmission1 = data.find('dt', class_='value-title', title='Коробка передач')
        if transmission1:
            transmission = transmission1.find_next_sibling('dd').text.strip()
        privod1 = data.find('dt', class_='value-title', title='Привод')
        if privod1:
            privod = privod1.find_next_sibling('dd').text.strip()
        wheel1 = data.find('dt', class_='value-title', title='Руль')
        if wheel1:
            wheel = wheel1.find_next_sibling('dd').text.strip()
        rastamozhka1 = data.find('dt', class_='value-title', title='Растаможен в Казахстане')
        if rastamozhka1:
            rastamozhka = rastamozhka1.find_next_sibling('dd').text.strip()
        main.loc[len(main)] = [model, year, price, city, generation, kuzov, engine, probeg, transmission, privod, wheel,rastamozhka]
        count += 1
        if count % 20 == 0:
            sleep(60)

    name = f'/opt/airflow/etl_data/Toyota_file{datetime.now().date()}.csv'
    main.to_csv(name, index=False)
    print('закончили с парсингом')
    return name

def transform_data(name):
    auto = pd.read_csv(name)
    auto['probeg'] = pd.to_numeric(auto['probeg'].str.strip(' км').str.replace(' ', ''))
    auto['privod'] = auto['privod'].str.strip(' привод')
    auto['model'] = auto['model'].str.replace('Toyota', '').str.strip()
    auto['price'] = pd.to_numeric(auto['price'].str.replace(r'\s+', '', regex=True).str.replace('\n', '').str.strip('₸').str.strip())
    auto = auto.rename(columns={'wheel': 'rulevoe_koleso', 'probeg': 'probeg_km', 'price': 'price_tg'})
    auto.to_csv(name, index=False)
    return name


def load_data(name):
    main = pd.read_csv(name)
    hook = PostgresHook(postgres_conn_id= 'airflow_postgres_id')
    engine = hook.get_sqlalchemy_engine()
    try:
        main.to_sql('toyota_file', con=engine, if_exists='append', index=False)
    except Exception as e:
        print(f'Братан тут {e}, нужно исправить недочёт')
    else:
        print('Файл загружен')

    try:
        with engine.connect() as connection:
            result = connection.execute(text("select * from toyota_file limit 5"))
            print(result.all())
    except Exception as e:
        print(f'{e}  что-то не то с таблицей')

default_args = {
    'owner': 'Aza',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='etl_kolesa_dag',
    start_date= datetime(2025,5, 29),
    schedule = '0 22 * * *',
    description= 'no description',
    default_args= default_args,
    catchup= False
 )as dag:
    @task
    def extract_data_task():
        extracted_data = extract_data()
        return extracted_data
    @task
    def transform_data_task(extracted_file):
        transformed_data = transform_data(extracted_file)
        return transformed_data
    @task
    def load_data_task(transformed_data):
        load_data(transformed_data)

    extract = extract_data_task()
    transform = transform_data_task(extract)
    load = load_data_task(transform)

    extract >> transform >> load
