from pyspark.sql import *

if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("Hello Spark")
             .master("local[2]")
             .getOrCreate())

    data_list = [("Luka",  28),
                 ("Ram", 27),
                 ("David", 58),
                 ("Ronn", 23),
                 ("Fil", 59)]

    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    df.show()
