package com.ash.com.SecondarySort

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
/**
  * Created by AHora2 on 9/5/2017.
  */

object SecondarySortRDD {
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


     /* using RDD*/
     val secondarySortRDD=df.rdd

     val nameToValue=secondarySortRDD.map(x=>{
       (x.getString(0),(x.getLong(1)+","+x.getInt(2)))
     })

     val nameGroupedToValue=nameToValue.groupByKey();

     val nameGroupedToSortedValue=nameGroupedToValue.mapValues(x=>{
       x.toSeq.sorted
     })
   // df.show();
     nameGroupedToSortedValue.foreach(x=>{
       x._2.foreach(y=>{
         println(x._1+","+y)
       })
     })
    /*
      Create RDD with 4 partitions
     */


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
