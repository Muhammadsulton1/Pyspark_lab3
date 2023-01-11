from src.process import DataMaker
from src.model import klustering
import pandas as pd
from src.to_db import to_ms_sql
from config import DATABASE,SERVER,PWD,UID

table_name = "Kmeans_lab3_data"
DM = DataMaker('data\laba_3.csv')
data_scale_output = DM.make_data()

new_data = klustering(data_scale_output)
to_ms_sql(new_data, table_name, DATABASE,SERVER,PWD,UID)
