import sys
import os
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix, RowMatrix
from pyspark.ml.linalg import Vectors
from pyspark import SparkContext, SparkConf
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import isnan, when, count, col, udf
from pyspark.sql.types import *



conf = SparkConf()
conf.set("spark.ui.port", "4050")
conf.set("spark.app.name", "third_lab_danilov")
conf.set("spark.master", "local")
conf.set("spark.executor.cores", "12")
conf.set("spark.worker.cores", "12")
conf.set("spark.executor.instances", "1")
conf.set("spark.executor.memory", "16g")
conf.set("spark.locality.wait", "0")
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryoserializer.buffer.max", "2000")
conf.set("spark.executor.heartbeatInterval", "6000s")
conf.set("spark.network.timeout", "10000000s")
conf.set("spark.shuffle.spill", "true")
conf.set("spark.driver.memory", "16g")
conf.set("spark.driver.maxResultSize", "16g")

# create the context
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()


"""class for preparing data. reading, cleaning, transforming, scaling"""
class DataMaker:
    def __init__(self, path):
        self.path = path
    def make_data(self):
        df = spark.read.csv(self.path, header=True).repartition(60)
        print("data readed")

        df = df.drop(*filter(lambda  col: '100g' not in col, df.columns))
        df = df.select([col(column).cast('float') for column in df.columns])
        print("data filtered and casted")
        print("shape of the data", (df.count(), len(df.columns)))

        df_filtered = df.na.fill(0)
        assemble = VectorAssembler(inputCols=df_filtered.columns, outputCol='features')
        assembled_data = assemble.transform(df_filtered)
        print("data transformed to verctorassmebler")

        scale = MinMaxScaler(inputCol='features', outputCol='standardized')
        data_scale = scale.fit(assembled_data)
        data_scale_output = data_scale.transform(assembled_data)
        print("data scaled")

        df.persist();
        df_filtered.persist();
        data_scale_output.persist();

        return data_scale_output

