import os
import io
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, desc, dense_rank
from pyspark.sql.window import Window
from zipfile import ZipFile
from datetime import timedelta

def zip_extract(x):
    """
    Функція для розпакування ZIP-архіву та зчитування CSV-файлів у pandas DataFrame.
    Повертає словник, де ключі - назви CSV-файлів, а значення - відповідні pandas DataFrame.
    Використовується при обробці багатофайлових ZIP-архівів.
    """
    in_memory_data = io.BytesIO(x[1])
    file_obj = ZipFile(in_memory_data, "r")
    csv_data = {}

    for file in file_obj.namelist():
        if file.lower().endswith('.csv'):
            csv_content = file_obj.read(file)
            csv_data[file] = pd.read_csv(io.StringIO(csv_content.decode('ISO-8859-1')))

    return csv_data

def process_zip_files(data_path):
    """
    Основна функція для обробки ZIP-архівів та запуску обробки даних.
    Використовується Spark для обробки багатофайлових ZIP-архівів паралельно.
    """
    spark = SparkSession.builder.appName("AppName").enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    zips = sc.binaryFiles(f"{data_path}/*.zip")

    files_data = zips.map(zip_extract).collect()

    for data in files_data:
        for _, content in data.items():
            if 'merged_df' not in locals():
                merged_df = spark.createDataFrame(content)

    merged_df.show()
    process_trip_data(merged_df)
    spark.stop()

def process_trip_data(trips_df):
    """
    Функція для виклику окремих завдань обробки даних.
    Може бути розширена для виклику нових завдань.
    """
    task1(trips_df)
    task2(trips_df)
    task3(trips_df)
    task4(trips_df)
    task5(trips_df)
    task6(trips_df)

def task1(trips_df):
    """
    Завдання 1: Обчислення середньої тривалості поїздок за день.
    """
    result_df = (
        trips_df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day"))
        .agg({"tripduration": "avg"})
        .orderBy("day")
    )
    to_csv(result_df, "average_trip_duration_per_day")

def task2(trips_df):
    """
    Завдання 2: Обчислення кількості поїздок за день.
    """
    result_df = (
        trips_df.groupBy(date_format("start_time", "yyyy-MM-dd").alias("day"))
        .count()
        .orderBy("day")
    )
    to_csv(result_df, "trips_count_per_day")

def task3(trips_df):
    """
    Завдання 3: Знаходження найпопулярнішої станції для початку подорожі за місяць.
    """
    result_df = (
        trips_df.withColumn("month", date_format("start_time", "yyyy-MM"))
        .groupBy("month", "from_station_name")
        .count()
        .orderBy("month", col("count").desc())
        .groupBy("month")
        .agg({"from_station_name": "first"})
        .orderBy("month")
        .withColumnRenamed("first(from_station_name)", "most_popular_starting_station")
    )
    to_csv(result_df, "most_popular_starting_station_per_month")

def task4(trips_df):
    """
    Завдання 4: Знаходження топ-3 станцій за кількістю поїздок за останні два тижні.
    """
    df = filter_last_two_weeks(trips_df)
    station_counts = df.groupBy("from_station_id", "from_station_name").count()
    window_spec = Window.orderBy(desc("count"))
    ranked_stations = station_counts.withColumn("rank", dense_rank().over(window_spec))
    top_stations = ranked_stations.filter(col("rank") <= 3)
    to_csv(top_stations, "top_three_stations_per_day_last_two_weeks")

def filter_last_two_weeks(trips_df):
    """
    Допоміжна функція для фільтрації даних за останні два тижні.
    """
    df = trips_df.withColumn("start_time", col("start_time").cast("timestamp"))
    end_date = df.agg({"start_time": "max"}).collect()[0][0]
    start_date = end_date - timedelta(days=14)
    return df.filter((col("start_time") >= start_date) & (col("start_time") <= end_date))

def task5(trips_df):
    """
    Завдання 5: Обчислення середньої тривалості поїздок за статевою ознакою.
    """
    result_df = (
        trips_df.groupBy("gender")
        .agg({"tripduration": "avg"})
        .orderBy("gender")
    )
    to_csv(result_df, "average_trip_duration_by_gender")

def task6(trips_df):
    """
    Завдання 6: Аналіз середньої тривалості поїздок за віком.
    """
    result_df = (
        trips_df.groupBy("birthyear")
        .agg({"tripduration": "avg"})
        .orderBy(col("avg(tripduration)").desc())
        .limit(10)
    )
    to_csv(result_df, "age_stats_top_ten")

def to_csv(dataframe, folder_name):
    """
    Допоміжна функція для збереження результатів в CSV-файл.
    """
    folder_path = f"reports/{folder_name}"
    dataframe.write.csv(folder_path, header=True, mode="overwrite")

def main():
    folder = 'data'
    current_directory = os.getcwd()
    data_path = os.path.join(current_directory, folder)
    process_zip_files(data_path)


if __name__ == "__main__":
    main()