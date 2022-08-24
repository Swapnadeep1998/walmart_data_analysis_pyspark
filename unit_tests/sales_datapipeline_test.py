from sales_datapipeline import WalmartDataPipeline
from pyspark_test import PySparkTest
import pandas as pd
from utility import VirtualDFPath


class TestWalmartDataPipeline(PySparkTest):

    @classmethod
    def setUp(cls) -> None:
        super().setUpClass()
        cls.arg_dict["SPARK_SESSION"] = cls.spark
        cls.pipeline = WalmartDataPipeline(arg_dict=cls.arg_dict)

    def test_load_calendar_data(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "d": ["d_1", "d_2", "d_3"]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "day": ["d_1", "d_2", "d_3"]
                }
            )
        )
        self.arg_dict['CALENDAR_DATA'] = VirtualDFPath(df_inp)
        df_act = self.pipeline.load_calendar_data(self.arg_dict)
        self.assertDataframeEqual(df_exp, df_act)

    def test_load_sales_price_data(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "sales": [2, 3, 4]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "sales": [2, 3, 4]
                }
            )
        )
        self.arg_dict['SALES_PRICE_DATA'] = VirtualDFPath(df_inp)
        df_act = self.pipeline.load_sales_price_data(self.arg_dict)
        self.assertDataframeEqual(df_exp, df_act)

    def test_load_sales_details_data(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "sales": [2, 3, 4]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "col1": ["a", "b", "c"],
                    "sales": [2, 3, 4]
                }
            )
        )
        self.arg_dict['SALES_DETAILS_DATA'] = VirtualDFPath(df_inp)
        df_act = self.pipeline.load_sales_details_data(self.arg_dict)
        self.assertDataframeEqual(df_exp, df_act)

    def test_select_1st_n_days(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "id": ["a", "b", "c"],
                    "item_id": ["a", "b", "c"],
                    "dept_id": ["a", "b", "c"],
                    "cat_id": ["a", "b", "c"],
                    "store_id": ["a", "b", "c"],
                    "state_id": ["a", "b", "c"],
                    "d_1": [0, 5, 10],
                    "d_2": [0, 5, 10],
                    "d_3": [0, 5, 10],
                    "d_4": [0, 5, 10],
                    "d_5": [0, 5, 10]
                }
            )
        )
        df_act = self.pipeline.select_1st_n_days(df_inp, 2)
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "id": ["a", "b", "c"],
                    "item_id": ["a", "b", "c"],
                    "dept_id": ["a", "b", "c"],
                    "cat_id": ["a", "b", "c"],
                    "store_id": ["a", "b", "c"],
                    "state_id": ["a", "b", "c"],
                    "d_1": [0, 5, 10],
                    "d_2": [0, 5, 10]
                }
            )
        )
        self.assertDataframeEqual(df_exp, df_act)

    def test_unpivot_day_wise_sales_data(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "id": ["a", "b"],
                    "item_id": ["a", "b"],
                    "dept_id": ["a", "b"],
                    "cat_id": ["a", "b"],
                    "store_id": ["a", "b"],
                    "state_id": ["a", "b"],
                    "d_1": [5, 10],
                    "d_2": [5, 10],
                    "d_3": [5, 10]

                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'id': ['a', 'a', 'a', 'b', 'b', 'b'],
                    'item_id': ['a', 'a', 'a', 'b', 'b', 'b'],
                    'dept_id': ['a', 'a', 'a', 'b', 'b', 'b'],
                    'cat_id': ['a', 'a', 'a', 'b', 'b', 'b'],
                    'store_id': ['a', 'a', 'a', 'b', 'b', 'b'],
                    'state_id': ['a', 'a', 'a', 'b', 'b', 'b'],
                    'day': ['d_1', 'd_2', 'd_3', 'd_1', 'd_2', 'd_3'],
                    'units_sold': [5, 5, 5, 10, 10, 10]
                }
            )
        )
        df_act = self.pipeline.unpivot_day_wise_sales_data(df_inp, 3)
        self.assertDataframeEqual(df_exp, df_act)

    def test_get_master_data(self):
        df_inp_a = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "store_id": ['a', 'b', 'c'],
                    "item_id": ['i1', 'i2', 'i3']
                }
            )
        )
        df_inp_b = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "store_id": ['a', 'b', 'c'],
                    "item_id": ['i1', 'i2', 'i3'],
                    "day": ["d_1", "d_2", "d_3"],
                    "wm_yr_wk": [10, 11, 12]
                }
            )
        )
        df_inp_c = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    "day": ["d_1", "d_2", "d_3"],
                    "wm_yr_wk": [10, 11, 12]
                }
            )
        )
        df_act = self.pipeline.get_master_data(df_inp_a, df_inp_b, df_inp_c)
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'day': ['d_1', 'd_3', 'd_2'],
                    'wm_yr_wk': [10, 12, 11],
                    'store_id': ['a', 'c', 'b'],
                    'item_id': ['i1', 'i3', 'i2']
                }
            )
        )
        self.assertDataframeEqual(df_exp, df_act)

    def test_get_price_earned_per_prod_per_store(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'id': ['1', '2', '3'],
                    'sell_price': [10, 20, 30],
                    'units_sold': [1, 2, 3]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'id': ['1', '2', '3'],
                    'sell_price': [10, 20, 30],
                    'units_sold': [1, 2, 3],
                    'revenue': [10, 40, 90]
                }
            )
        )
        df_act = self.pipeline.get_price_earned_per_prod_per_store(df_inp)
        self.assertDataframeEqual(df_exp, df_act)

    def test_get_top_n_stores(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'store_id': ['1', '2', '3'],
                    'sell_price': [10, 20, 30],
                    'units_sold': [1, 2, 3],
                    'revenue': [10, 40, 90]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'store_id': ['3', '2'],
                    'total_revenue': [90, 40]
                }
            )
        )
        df_act = self.pipeline.get_top_n_stores(df_inp, n_stores=2)
        self.assertDataframeEqual(df_exp, df_act)

    def test_get_total_units_of_each_prod_per_store(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'store_id': ['1', '2', '3'],
                    'item_id': ['i1', 'i2', 'i3'],
                    'sell_price': [10, 20, 30],
                    'units_sold': [1, 2, 3],
                    'revenue': [10, 40, 90]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'store_id': ['3', '2', '1'],
                    'item_id': ['i3', 'i2', 'i1'],
                    'total_units_sold': [3, 2, 1]
                }
            )
        )
        df_act = self.pipeline.get_total_units_of_each_prod_per_store(df_inp)
        self.assertDataframeEqual(df_exp, df_act)

    def test_get_2_best_selling_prods_per_store(self):
        df_inp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'store_id': ['3', '3', '3', '2', '2', '2', '1', '1', '1'],
                    'item_id': ['i3', 'i2', 'i1', 'i3', 'i4', 'i3', 'i1', 'i2', 'i1'],
                    'total_units_sold': [3, 20, 10, 40, 20, 35, 60, 4, 20]
                }
            )
        )
        df_exp = self.spark.createDataFrame(
            pd.DataFrame(
                {
                    'store_id': ['3', '1', '2'],
                    'TOP_PRODUCT_1': ['i2', 'i1', 'i3'],
                    'TOP_PRODUCT_2': ['i1', 'i1', 'i3']
                }
            )
        )
        df_act = self.pipeline.get_2_best_selling_prods_per_store(df_inp)
        self.assertDataframeEqual(df_exp, df_act)





