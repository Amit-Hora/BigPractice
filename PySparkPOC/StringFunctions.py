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
from pyspark.sql.functions import expr, pow, locate
from pyspark.sql.functions import round,bround, mean, max
from pyspark.sql.functions import current_date, current_timestamp,date_add,date_sub,to_date, split, explode

def create_spark_session(app_name="SparkApplication"):
    spark_session = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    spark_session.sparkContext.setLogLevel("WARN")

    return spark_session

def color_locator(column, color_string):
  """This function creates a column declaring whether or
  not a given pySpark column contains the UPPERCASED
  color.

  Returns a new column type that can be used
  in a select statement.
  """
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + c)


def color_locator(column, color_string):
    """This function creates a column declaring whether or
    not a given pySpark column contains the UPPERCASED
    color.

    Returns a new column type that can be used
    in a select statement.
    """
    return locate(color_string, column) \
        .cast("boolean") \
        .alias("is_" + color_string)

def main():

    session = create_spark_session()
    sc = session.sparkContext
    myschema = StructType([
        StructField("Description", StringType(), False)
    ])
    myRowwhite=Row("white this is one color of white")
    myRowblack = Row("black this is one color of black")
    mydf=session.createDataFrame([myRowwhite,myRowblack],myschema)
    simpleColors=["black","white"]
    selectedColumns = [color_locator(mydf.Description, c) for c in simpleColors]
    selectedColumns.append(expr("*"))  # has to a be Column type
    mydf \
        .select(*selectedColumns) \
        .where(expr("is_white OR is_black")) \
        .select("Description") \
        .show(3, False)
# working with date and timestamp

    datedf=session.range(10) \
                  .withColumn("today",to_date(lit('2017-20-12'))) \
                .withColumn("today1", to_date(lit('2017-20-12'))) \
                .withColumn("now",current_timestamp())
    datedf.show()
    datedf.na.drop(subset=['today']).show()
    datedf.na.fill({'today':'1945-05-05'}).show()


    beforeexploddf=session.range(1).withColumn('employees',lit('e1,528#e2,529')) \
                              .withColumn('manager',lit('m1'))

    afterexploddf=beforeexploddf.withColumn('splitted',split(col('employees'),'#')) \
                                .withColumn('exploded',explode('splitted')) \
                                .withColumn('splitExploded', split(col('exploded'), ',')) \
                                .selectExpr('manager', 'splitExploded[0]','splitExploded[1]')
#                                .withColumn('splitExploded',split(col('exploded'),',')) \
#                                .withColumn('explodedExplodes',explode('splitExploded')) \

    afterexploddf.show()
# add and delete 5 days
#    datedf.select(
 #       date_add('today',5),
 #       date_sub('today',5)
 #   ).show()
if __name__ == '__main__':
    main()