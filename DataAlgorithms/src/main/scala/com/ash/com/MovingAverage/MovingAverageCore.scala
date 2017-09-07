package com.ash.com.MovingAverage

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
/**
  * Created by AHora2 on 8/30/2017.
  */
object MovingAverageCore extends App {
  override def main(args: Array[String]): Unit = {
    val spark = getSparkSession("local[*]")
    val stockeValues = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
      55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100)
    /*
    Create RDD with 4 partitions
   */
    //val stockValueRDD = spark.sparkContext.parallelize(stockeValues.toSeq, 4);
    val ts = spark.sparkContext.parallelize(1 to 100, 4)
    val window = 3
    val partitioned = ts.mapPartitionsWithIndex((i, p) => {
   /*   p.foreach(x=>{
        println(i+",, "+ x)
      })*/
      val overlap = p.take(window-1).toArray

   //   println(overlap.deep.mkString(",,,,,"+i+"\n"))
      val spill = overlap.iterator.map((i - 1, _))

     /* spill.foreach(x=>{
          print(x._1+" ,,, "+x._2)
      })*/
    //  val overlap2=overlap.iterator ++ p
      /*overlap2.foreach(x=>{
        if(i == 1)
        println(i+",,,,"+x)
      })*/
     // println(overlap2.deep.mkString(",,,,,"+i+"\n"))
      val keep = (overlap.iterator ++ p).map((i, _))
      /*keep.foreach(x=>{
        println(x._1+" ,,,, " + x._2)
      })*/
      if (i == 0) {
        /*keep.foreach(x=>{
          println(i+",,,,"+x)
        })*/
        keep
      }
        else {

          keep ++ spill

        }/*println((keep ++ spill).foreach(x=>{
          println(i+",,,,,"+x)
        }))*/
    }).partitionBy(new StraightPartitioner(ts.partitions.length)).values

   /*partitioned.mapPartitionsWithIndex((i, p) => {
      p.toSeq.sorted.foreach(x=>{
        println(i+",,,,,"+x)
      })
     p
    }).collect()*/
   /* partitioned.foreachPartition(x=>{
      println("**********")
      x.foreach(println)
    })*/
    val movingAverage = partitioned.mapPartitions(p => {
      val sorted = p.toSeq.sorted
      val olds = sorted.iterator
      val news = sorted.iterator
  /*   olds.foreach(x=>{
       println("OLC "+ x)
     })

     news.foreach(x=>{
       println("News "+ x)
     })*/
     print("Partition Change")
      var sum = news.take(window - 1).sum
     //println("Partition Change SUM "+ sum)
     // println(sum)
      (olds zip news).map({ case (o, n) => {
      //  print(" "+n)
        if(sum ==3){
          println(n)
        }
        sum += n
        val v = sum
        sum -= o
        v/3
      }})
    })

    movingAverage.collect/*.sameElements(3 to 297 by 3)*/

   /*   val valuesRDD=partitioned.map(x=>{

      (x._1,x._2)
    })
    val groupedRDD=valuesRDD.groupByKey();


    groupedRDD.foreachPartition(x=>{
       x.foreach(y=>{
         var count=0;
         var sum=0;
         var old=0;
         var temp=0;
         var a=0;
         var b=0;
         var c=0;
         y._2.toSeq.sorted.foreach(v=>{
           // actual value
           println("This is V"+v)
           sum=sum+v;
           count=count+1;

          if(temp==0){
            a=v;
          }
          else if(temp ==1){
             b=v;
           }
           else{
            c=v;
          }
           temp=temp+1;
           if(count%3==0){
             count=count-1;
            // count=count-1;
             println((a+b+c)/3)
             println(a+",,,,,"+",,,,"+b+",,,,"+c)
             //sum=sum-old;
             a=b;
             b=c;
           }
         })
       })
     })*/

   /*groupedRDD.foreach(x=>{
      val sorted=x._2.toSeq.sorted
      var count=0;
      var sum=0;
      var old=0;
      var a=0;var b=0;
      sorted.foreach(v=>{
        sum=sum+v;
        count=count+1;
        if(count==0){
          old=v;
        }
        if(count==3){

          count=0;
          println(sum/3)
          sum=sum-old;
        }

      })
    })*/

    /*groupedRDD.foreach(x=>{
      x._2.foreach(y=>{
        println(x._1+",,,," +y)
      })
      //println(y)
    })*/
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



class StraightPartitioner(p: Int) extends Partitioner {
  def numPartitions = p //4
  override def getPartition(key : scala.Any) : scala.Int= (key.asInstanceOf[scala.Int] * p/4).asInstanceOf[Int]

}


/*val partitioned = ts.mapPartitionsWithIndex((i, p) => {
val overlap = p.take(window - 1).toArray
val spill = overlap.iterator.map((i - 1, _))
val keep = (overlap.iterator ++ p).map((i, _))
if (i == 0) keep else keep ++ spill
}).partitionBy(new StraightPartitioner(ts.partitions.length)).values

val movingAverage = partitioned.mapPartitions(p => {
val sorted = p.toSeq.sorted
val olds = sorted.iterator
val news = sorted.iterator
var sum = news.take(window - 1).sum
(olds zip news).map({ case (o, n) => {
sum += n
val v = sum
sum -= o
v
}})
})

scala> movingAverage.collect.sameElements(3 to 297 by 3)
res0: Boolean = true*/
