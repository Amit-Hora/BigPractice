import os
import sys

os.environ['SPARK_HOME'] = "C:\\Users\\ahora2\\Downloads\\spark-2.1.1-bin-hadoop2.7\\spark-2.1.1-bin-hadoop2.7"
os.environ['JAVA_HOME']="C:\\Progra~1\\\Java\\jdk1.8.0_102"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext



def main():
    sc=SparkContext("local[2]","NetworkWordCount")
    ssc=StreamingContext(sc,1)
    # batch interval of 1 second
    lines=ssc.socketTextStream("localhost",9999)
    words=lines.flatMap(lambda line : line.split(" "))
    #flatmap is one to many operation that creates new DStream by generating multiple new records
    # for every record in old DStream

    # count each word in each batch
    pairs= words.map(lambda word: (word,1))
    wordCounts= pairs.reduceByKey(lambda  x , y : x+y)

    # print the first ten elements generated
    wordCounts.pprint()
    ssc.start()
    ssc.awaitTermination()







if __name__ == '__main__':
    main()