# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
import re

# COMMAND ----------

spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .appName("spark_wordcount_databrick") \
        .getOrCreate()

# COMMAND ----------

sc = spark.sparkContext
sc

# COMMAND ----------

rdd = sc.textFile("dbfs:/FileStore/shared_uploads/19447201.iuh@gmail.com/shakespeare.txt",8)\
        .map(lambda x: x.strip()) \
        .filter(lambda x: len(x)>0)
rdd.collect()


# COMMAND ----------

# MAGIC %md
# MAGIC #Splitting

# COMMAND ----------

splitRDD= rdd.flatMap(lambda x: x.split(" "))

# COMMAND ----------

splitRDD_WordCount = splitRDD.count()
print(splitRDD_WordCount)

# COMMAND ----------

# MAGIC %md
# MAGIC #Chuyển các từ trong splitRDD thành chữ thường, loại bỏ các ký tự đặc biệt, stopword từ file chứa danh sách các stop_words có sẵn

# COMMAND ----------

def removePunctuation(text):
  t1 = text.lower()
  t2 = re.sub(r'[^0-9a-z\s]',"",t1)
  t3 = t2.strip()
  return t3

# COMMAND ----------

# dbfs:/FileStore/shared_uploads/19447201.iuh@gmail.com/stopwords
stop_words = sc.textFile("/FileStore/shared_uploads/19447201.iuh@gmail.com/stopwords.txt")
lst_stop_words =stop_words.collect()

# COMMAND ----------

splitRDD_stopwords = splitRDD.map(removePunctuation).filter(lambda x:  x not in lst_stop_words)

# COMMAND ----------

# splitRDD_WordCount = splitRDD_stopwords.count()
print(splitRDD_stopwords.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #Mapping

# COMMAND ----------

wordPairs = splitRDD_stopwords.map(lambda x: (x, 1))
# print(wordPairs.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC #Reduce

# COMMAND ----------

resultRDD = wordPairs.reduceByKey(lambda x,y : x+y)
print (resultRDD.collect())

# COMMAND ----------

resultRDD.saveAsTextFile("hdfs://namenode:9000/project/spark_hadoop/wordcount/data/output/result");
