package com.ash.com.MovingAverage

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
/**
 * @author Amit!
 *
 */
final case class StockValue(price: Int)
object MovingAverageApp extends App {
  override def main(args: Array[String]): Unit = {

    val spark = getSparkSession(args(0))
    val stockeValues= Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,
      55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100)

    import spark.implicits._
    /*
      Create RDD with 4 partitions
     */
    val stockValueRDD=spark.sparkContext.parallelize(stockeValues.toSeq,4);

    val stockValueCaseRdd=stockValueRDD.map(price=>StockValue(price))

    val stockValueDataFrame=stockValueCaseRdd.toDF();
    /**/
    /*
      Setting 3 rows windows , current rows and 2 rows ahead
     */

    val wSpec1 = Window.rowsBetween(0,2)
    val stockValueAvgDS=stockValueDataFrame.withColumn( "priceavg",
      avg(stockValueDataFrame("price")).over(wSpec1)).withColumn( "count",
      count(stockValueDataFrame("price")).over(wSpec1))
    /*
      As we need to show average for a window of 3 rows, filter out all rows where rowcount is not three for that window
     */
    val stockValueAvgDSfiltered= stockValueAvgDS.filter("count=3")
    stockValueAvgDSfiltered.collect().foreach(r=>{
      println(r.getDouble(1))
    })
    //stockValueDataSet.printSchema()

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
