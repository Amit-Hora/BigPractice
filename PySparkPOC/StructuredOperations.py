import os
import sys

os.environ['SPARK_HOME'] = "C:\\Users\\ahora2\\Downloads\\spark-2.1.1-bin-hadoop2.7\\spark-2.1.1-bin-hadoop2.7"
os.environ['JAVA_HOME']="C:\\Progra~1\\\Java\\jdk1.8.0_102"

from random import random
from pyspark.sql import SparkSession ;
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType
from array import array
from pyspark.sql import Row
from pyspark.sql.functions import col, column,expr, lit
from pyspark.sql.functions import instr
from pyspark.sql.functions import expr, pow
from pyspark.sql.functions import round,bround, mean, max

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
    schema=StructType(
        [StructField("Sno", LongType(), True), StructField("Name", StringType(), True), StructField("Place", StringType(), True),
             StructField("Area", StringType(), True), StructField("code", StringType(), True), StructField("geocode", StringType(), True),
             StructField("lat", StringType(), True), StructField("lon", StringType(), True),
             StructField("no", StringType(), True)])

    df=session.read.format("csv").option("header", "true").schema(schema).load("file:///C:\pluralsight\\airports.csv")
   # df.show()
    #.col("Sno")
    sno=df.select(expr('Sno')).collect()

    myschema = StructType([
        StructField("Sno",StringType(),False),
        StructField("code",IntegerType(),True)
    ])
    myRow=Row("101",1)
    mydf=session.createDataFrame([myRow],myschema)
    mydf.createOrReplaceTempView("mydf")
    #session.sql("Select * from mydf where code >= 1").show(1)
    mydfwithcolname=mydf.withColumn("This Long Column-Name",lit(1))
    mydfwithcolname.show()
    mydfwithcolname \
        .selectExpr(
        "`This Long Column-Name`",
        "`This Long Column-Name` as `new col`") \
        .show(2)

    mydfwithcolname.drop("This Long Column-Name").show()
    df.selectExpr("count(Sno) as count").show();
    #mydf.filter(col('code')<2).take(2)
    #mydf.where(col('code') == 1).select("Sno").show(5,False)

    #session.sql("Select * from mydf where instr(Sno,'20')>=1 and (code=1 or Sno='101')").show()

    fabricatedcol=pow(col("code")*2,2)+5
    mydf.select(
        expr('Sno'),
        fabricatedcol.alias('multiplied')
    ).show()
    #mydf.select([mean('code')]).show()
    mydf.groupBy('Sno').agg({"code":"mean"}).show()
    #mydf.show()
    #for y in mydf.select("Sno","code").collect():
    #    print(y)

    #for x in sno:
      #  print(x)

    #mydf.select(expr("*"),lit(1).alias("one"),expr("code < one")).show(1)
   # mydf.select(expr("Sno,code"),expr("Sno > 1")).show(1)

if __name__ == '__main__':
    main()