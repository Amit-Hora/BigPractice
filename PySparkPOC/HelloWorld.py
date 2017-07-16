import os
import sys

os.environ['SPARK_HOME'] = "C:\\Users\\ahora2\\Downloads\\spark-2.1.1-bin-hadoop2.7\\spark-2.1.1-bin-hadoop2.7"
os.environ['JAVA_HOME']="C:\\Progra~1\\\Java\\jdk1.8.0_102"

from random import random
from pyspark.sql import SparkSession ;


def create_spark_session(app_name="SparkApplication"):
    spark_session = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("WARN")

    return spark_session


def main():
    session = create_spark_session()
    sc = session.sparkContext
    myRange=session.range(100).toDF("number")
    myRange.show()
    for x in myRange.collect():
        print(x)

    myRange.createOrReplaceTempView("Range")
    count=session.sql("""SELECT max(number) from Range""")
    print(count)
    count.explain()

if __name__ == '__main__':
    main()


