from pyspark.sql import SparkSession
import yaml
from sales_datapipeline import WalmartDataPipeline
yaml_path = r"D:\SoftwareDevelopment\walmart_analysis\data_pipeline\master-config.yaml"

with open(yaml_path, 'r') as yaml_file:
    arg_dict = yaml.load(yaml_file, yaml.BaseLoader)

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Walmart-Sales').getOrCreate()
    module_name = "WalmartSales"
    arg_dict = arg_dict[module_name]
    arg_dict["SPARK_SESSION"] = spark
    data_pipeline = WalmartDataPipeline(arg_dict=arg_dict)
    data_pipeline.execute()
