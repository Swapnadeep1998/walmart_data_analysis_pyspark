from pyspark.sql import DataFrame
from pyspark.sql import functions as psf


class VirtualDFPath:
    def __init__(self, df: DataFrame):
        self.df = df


class Utility:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs['arg_dict']
        self.spark = self.kwargs['SPARK_SESSION']
        self.psf = psf

    def read_csv(self, path: str) -> DataFrame:
        if isinstance(path, VirtualDFPath):
            return path.df
        else:
            df = self.spark.read.format('csv')\
                     .load(path, header=True, inferSchema=True)
        return df

    def write_parquet(self, df: DataFrame, path: str):
        if isinstance(path, VirtualDFPath):
            pass
        else:
            df.repartition(1).write.format('parquet').save(path, mode='overwrite')
        return



