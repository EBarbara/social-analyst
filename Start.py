import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="your_spark_home_folder"

# Append pyspark  to Python Path
sys.path.append("your_pyspark_folder ")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)