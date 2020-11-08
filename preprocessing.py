import glob
import operator
import os
import re
import shutil

import pyspark
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def init_folder(folder_path):
    shutil.rmtree(folder_path, ignore_errors=True)
    os.makedirs(folder_path)


def trans(s):
    """Transform Chinese numerals to Arabic numerals.
    assert total floor number is less than 1000.
    """
    digit = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}
    num = 0
    if s:
        if s[-1] == "層":
            s = s[:-1]
        idx_b, idx_s = s.find("百"), s.find("十")
        if idx_b != -1:
            num += digit[s[idx_b - 1 : idx_b]] * 100
        if idx_s != -1:
            # if start with "十", use 1 as default multiplier.
            num += digit.get(s[idx_s - 1 : idx_s], 1) * 10
        if s[-1] in digit:
            num += digit[s[-1]]
    else:
        return None
    return num


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LvrLandProcessing").getOrCreate()

    # City mapping from manifest.csv
    city_dict = {"a": "台北市", "b": "台中市", "e": "高雄市", "f": "新北市", "h": "桃園市"}
    file_list = [
        "tmp/unzip/a_lvr_land_a.csv",
        "tmp/unzip/b_lvr_land_a.csv",
        "tmp/unzip/e_lvr_land_a.csv",
        "tmp/unzip/f_lvr_land_a.csv",
        "tmp/unzip/h_lvr_land_a.csv",
    ]
    result_file_path = "tmp/result/result.json"

    result = None
    for file in file_list:
        city_name = city_dict.get(file.split("/")[-1][0])
        df = spark.read.option("header", True).csv(file)

        # Add city name
        df = df.withColumn("city", F.lit(city_name))

        # Rename columns
        new_column_name_list = list(map(lambda x: x.lower().replace(" ", "_"), df.columns))
        df = df.toDF(*new_column_name_list)
        df = df.withColumnRenamed("transaction_year_month_and_day", "date")
        df = df.withColumnRenamed("the_villages_and_towns_urban_district", "district")

        # date: 民國->西元, ISO format
        df = df.withColumn(
            "date",
            F.date_format(F.to_date((df.date + 19110000).cast("integer").cast("string"), "yyyyMMdd"), "yyyy-MM-dd"),
        )

        # Use UserDefinedFunctions to transform total floor number
        floor_udf = F.udf(lambda x: trans(x), IntegerType())
        df = df.withColumn("total_floor_number", floor_udf(df.total_floor_number))

        """Filter by
        -【主要用途】為【住家用】
        -【建物型態】為【住宅大樓】
        -【總樓層數】需【大於等於十三層】"""
        df = df.filter("main_use = '住家用' and building_state like '%住宅大樓%' and total_floor_number >= 13")
        df = df.select(df.city, df.date, df.district, df.building_state)
        if result:
            result = result.union(df)
        else:
            result = df

    # Shape required result format
    groupByWind = Window.partitionBy("city", "date")
    result = (
        result.withColumn("event", F.struct("district", "building_state"))
        .withColumn("events", F.collect_list("event").over(groupByWind))
        .withColumn("time_slots", F.struct("date", "events"))
        .groupBy("city")
        .agg(F.collect_set("time_slots").alias("time_slots"))
    )
    # Should have a better solution not to transform to RDD and back to DF.
    result = result.rdd.map(lambda x: [x[0], sorted(x[1], key=operator.itemgetter("date"), reverse=True)]).toDF(
        ["city", "time_slots"]
    )

    # Write result to 2 part files.
    result.coalesce(2).write.format("json").save(result_file_path)

    # Change to HDFS if needed.
    init_folder("result")
    for r in glob.glob(f"{result_file_path}/*.json"):
        n = int(re.search(r"part-(\d{5})-.+", r).group(1)) + 1
        shutil.copy(r, f"result/result-part{n}.json")

    print("Process completed!")
    spark.stop()
