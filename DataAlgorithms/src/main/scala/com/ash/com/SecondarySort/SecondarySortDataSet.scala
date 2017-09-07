package com.ash.com.SecondarySort

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
/**
  * Created by AHora2 on 9/5/2017.
  */

object SecondarySortDataSet {
  def main(args: Array[String]): Unit = {

    val spark = getSparkSession("local[*]")
    val schema = StructType(
      StructField("name", StringType, true) ::
        StructField("time", LongType, true)::
        StructField("value", IntegerType, true) :: Nil)
    val df = spark.
      read.
      option("header", true).
      schema(schema)
      .csv("C:\\maxwell\\MovingAverage\\src\\main\\resources\\secondarysort.csv")//.as[DataPoints];


    val sortedDF=df.sort("name","time")
    sortedDF.show(20)


  }

  def getSparkSession(host: String) : SparkSession={
    val spark = SparkSession
      .builder()
      .appName("MovingAverageApplication")
      .master(host)
      .getOrCreate();

    spark;
  }


}