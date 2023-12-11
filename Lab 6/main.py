import duckdb
import pandas as pd
import os
import fastparquet 

def create_table():
    # З'єднання з базою даних DuckDB
    connection = duckdb.connect(database=':memory:', read_only=False)

    # Визначення схеми таблиці
    table_schema = '''
    CREATE TABLE electric_cars (
        VIN VARCHAR(10),
        County VARCHAR,
        City VARCHAR,
        State VARCHAR(2),
        "Postal Code" INTEGER,
        "Model Year" INTEGER,
        Make VARCHAR,
        Model VARCHAR,
        "Electric Vehicle Type" VARCHAR,
        "CAFV Eligibility" VARCHAR,
        "Electric Range" INTEGER,
        "Base MSRP" DOUBLE, -- Змінено тип на DOUBLE
        "Legislative District" INTEGER,
        "DOL Vehicle ID" INTEGER,
        "Vehicle Location" VARCHAR,
        "Electric Utility" VARCHAR,
        "2020 Census Tract" VARCHAR
    );
    '''

    try:
        # Виконання SQL запиту для створення таблиці
        connection.execute(table_schema)
        print("Таблицю створено успішно.")
    except Exception as e:
        print(f"Помилка при створенні таблиці: {e}")

    return connection

def import_data(connection, file_path):
    try:
        # Зчитування даних з CSV файлу
        data = pd.read_csv(file_path)

        # Заміна nan значень на NULL
        data = data.where(pd.notna(data), None)

        # Вставка даних у таблицю
        connection.register('electric_cars', data)
        print("Дані успішно імпортовано.")
    except Exception as e:
        print(f"Помилка при імпорті даних: {e}")

def print_table(connection, limit=10):
    try:
        # Витягти перші 10 рядків з таблиці
        result = connection.execute(f"SELECT * FROM electric_cars LIMIT {limit}")

        # Отримати дані та вивести їх
        for row in result.fetchdf().itertuples():
            print(row)
    except Exception as e:
        print(f"Помилка при виведенні таблиці: {e}")

def analyze_and_save_results(connection):
    try:
        # Кількість електромобілів у кожному місті
        city_counts = connection.execute("SELECT City, COUNT(*) AS Count FROM electric_cars GROUP BY City").fetchdf()
        city_counts.to_parquet('results/city_counts.parquet', index=False)

        # Топ-3 електромобілі
        top_models = connection.execute("SELECT Model, COUNT(*) AS Count FROM electric_cars GROUP BY Model ORDER BY Count DESC LIMIT 3").fetchdf()
        top_models.to_parquet('results/top_models.parquet', index=False)

        # Найпопулярніший електромобіль у кожному поштовому індексі
        popular_by_postal = connection.execute("""
            SELECT 
                "Postal Code",
                ANY_VALUE(Make) AS Make,
                ANY_VALUE(Model) AS Model
            FROM electric_cars 
            GROUP BY "Postal Code"
        """).fetchdf()

        # Зберегти результат у файл
        popular_by_postal_result_path = 'results/popular_by_postal_simple.parquet'
        popular_by_postal.to_parquet(popular_by_postal_result_path, index=False, engine='fastparquet')

        # Кількість електромобілів за роками випуску
        cars_by_year = connection.execute("""
            SELECT "Model Year", COUNT(*) AS Count
            FROM electric_cars 
            GROUP BY "Model Year"
        """).fetchdf()
        cars_by_year_result_path = 'results/cars_by_year.parquet'
        cars_by_year.to_parquet(cars_by_year_result_path, index=False, engine='fastparquet')

        print("Аналітика виконана та результати збережено у відповідні файли в папці 'results'")
    except Exception as e:
        print(f"Помилка при аналізі та збереженні результатів: {e}")

def main():
    # Задати шлях до CSV файлу
    file_path = './data/Electric_Vehicle_Population_Data.csv'

    # Створити таблицю
    db_connection = create_table()

    if db_connection:
        # Імпортувати дані
        import_data(db_connection, file_path)

        # Вивести перші 10 рядків таблиці в консоль
        print("Перші 10 рядків таблиці:")
        print_table(db_connection, limit=10)

        # Виконати аналітику та зберегти результати у Parquet файли
        analyze_and_save_results(db_connection)

if __name__ == "__main__":
    # Створити папку для результатів, якщо вона не існує
    results_folder = 'results'
    if not os.path.exists(results_folder):
        os.makedirs(results_folder)
    
    main()