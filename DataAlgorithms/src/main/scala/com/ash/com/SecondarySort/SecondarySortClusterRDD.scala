package com.ash.com.SecondarySort

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
/**
  * Created by AHora2 on 9/5/2017.
  */
case class DataPoints (name: String, time: Long, value: Integer)
object SecondarySortClusterRDD {
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

      ((x.getLong(0)+","+x.getInt(1)),x.getString(2))
    })
    val nameGroupedToValue = nameToValue.repartitionAndSortWithinPartitions(new DataPartitioner(2))

    nameGroupedToValue.collect().foreach(println)


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
class DataPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[StringType]
    k.toString.split(",")(0).hashCode % numPartitions
  }
}