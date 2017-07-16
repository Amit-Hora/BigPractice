import sys
import os
from operator import add

os.environ['SPARK_HOME'] = "C:\\Users\\ahora2\\Downloads\\spark-2.1.1-bin-hadoop2.7\\spark-2.1.1-bin-hadoop2.7"
os.environ['JAVA_HOME']="C:\\Progra~1\\\Java\\jdk1.8.0_102"

# try the import Spark Modules
try:
    from pyspark.sql import SparkSession

    sparkSession=SparkSession.builder.master("local[*]").getOrCreate()
    myRange=sparkSession.range(10) \
            .toDF("number")



except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

# Run a word count example on the local machine:

