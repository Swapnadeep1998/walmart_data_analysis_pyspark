import unittest
import logging
from pyspark.sql import SparkSession, DataFrame
from pandas.testing import assert_frame_equal


class PySparkTest(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger("py4j")
        logger.setLevel(logging.ERROR)

    @classmethod
    def create_testing_spark_session(cls):
        spark = SparkSession.builder.appName("Test-Sales-DataPipeline")\
                            .master("local")\
                            .getOrCreate()
        return spark

    @classmethod
    def setUpClass(cls):
        cls.spark = cls.create_testing_spark_session()
        cls.arg_dict = {}

    def assertDataframeEqual(self, a, b, msg="data frames not equal"):
        if isinstance(a, DataFrame):
            print('\nExpected:')
            a.show()
            exp_pd = a.toPandas()
        if isinstance(b, DataFrame):
            print('\nActual:')
            b.show()
            act_pd = b.toPandas()

        try:
            assert_frame_equal(exp_pd, act_pd, check_dtype=False, check_index_type=False)
        except AssertionError as e:
            raise self.failureException(msg) from e

