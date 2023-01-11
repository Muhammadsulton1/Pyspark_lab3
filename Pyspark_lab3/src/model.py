from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd
import os

"""model for Kmeans clustering"""
def klustering(data_scale_output):
    """количество кластеров было выбранно методом локтя"""
    KMeans_algo = KMeans(featuresCol='standardized', k=10, predictionCol='prediction')
    KMeans_fit = KMeans_algo.fit(data_scale_output)
    output = KMeans_fit.transform(data_scale_output)
    print("Kmeans is ready")

    data_write = KMeans_fit.summary.predictions.drop('features', 'standardized')
    data_write = data_write.withColumnRenamed('prediction', 'labels')
    data_write.printSchema()

    data_csv = data_write.select("*").toPandas()
    data_csv.to_csv('data\lab3_cluster.csv')
    #data_write.repartition(1).write.format('com.databricks.spark.csv').save('lab3_cluster.csv', header='true')
    print("data was written into csv")

    return data_csv