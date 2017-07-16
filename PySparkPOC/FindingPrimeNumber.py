import os
import sys

os.environ['SPARK_HOME'] = "C:\\Users\\ahora2\\Downloads\\spark-2.1.1-bin-hadoop2.7\\spark-2.1.1-bin-hadoop2.7"
os.environ['JAVA_HOME']="C:\\Progra~1\\\Java\\jdk1.8.0_102"

from random import random
from pyspark.sql import SparkSession ;
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, BooleanType
from array import array
from pyspark.sql import Row
from pyspark.sql.functions import col, column,expr, lit
from pyspark.sql.functions import instr
from pyspark.sql.functions import expr, pow, locate
from pyspark.sql.functions import round,bround, mean, max
from pyspark.sql.functions import udf

def create_spark_session(app_name="SparkApplication"):
    spark_session = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("WARN")

    return spark_session

def isPrime(x):
    if x >= 2:
        for y in range(2, x):
            if not (x % y):
                return False
    else:
        return False
    return True


def isPrimeOrNot(x,isPrime1):
    isPrime1(x).cast("boolean") \
        .alias("is_Prime")




def main():

    session = create_spark_session()
    sc = session.sparkContext
#    myschema = StructType([
#        StructField("Description", StringType(), False)
#    ])
    isPrime1 = udf(isPrime)
    mydf = session.range(100).toDF("number")
    mynewdf=mydf.withColumn("is_Prime", isPrime1(mydf.number))
    mynewdf.createOrReplaceTempView("mynewdf")
    session.sql("select number,is_Prime from mynewdf ").show()
#    mynewdf.select("*").where(expr("is_Prime")).show()
    #isPrime1(mydf.number)
    #selectedColumns = [isPrimeOrNot(mydf.number,isPrime1)]
    #selectedColumns.append(expr("*"))  # has to a be Column type
   # mydf \
    #    .select(*selectedColumns) \
     #   .where(expr("is_Prime")) \
      #  .select("number") \
       # .show(100, False)
if __name__ == '__main__':
    main()

