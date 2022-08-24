from utility import Utility, VirtualDFPath
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


class WalmartDataPipeline(Utility):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.arg_dict = kwargs['arg_dict']

    def execute(self):
        cmd = self.arg_dict
        df_calendar = self.load_calendar_data(cmd)
        df_sales_price = self.load_sales_price_data(cmd)
        df_sales_details = self.load_sales_details_data(cmd)
        df_sales_details_100days = self.select_1st_n_days(df_sales_details, n_days=100)
        df_sales_details_100days_unpivotted = self.unpivot_day_wise_sales_data(df_sales_details_100days, n_days=100)
        df_master = self.get_master_data(df_sales_details_100days_unpivotted, df_sales_price, df_calendar)
        df_master_revenue = self.get_price_earned_per_prod_per_store(df_master)
        df_top5_stores = self.get_top_n_stores(df_master_revenue, n_stores=5)
        self.write_parquet(df_top5_stores, cmd['TARGET_TOP5_STORES'])
        df_total_units_sold_per_store_per_prod = self.get_total_units_of_each_prod_per_store(df_master_revenue)
        df_store_wise_top2_items = self.get_2_best_selling_prods_per_store(df_total_units_sold_per_store_per_prod)
        self.write_parquet(df_store_wise_top2_items, cmd['TARGET_TOP2_PRODS_STORES'])

    def load_calendar_data(self, cmd: dict) -> DataFrame:
        df = self.read_csv(cmd['CALENDAR_DATA'])
        df_calendar = df.withColumnRenamed('d', 'day')
        return df_calendar

    def load_sales_price_data(self, cmd: dict) -> DataFrame:
        df = self.read_csv(cmd['SALES_PRICE_DATA'])
        return df

    def load_sales_details_data(self, cmd: dict) -> DataFrame:
        df = self.read_csv(cmd['SALES_DETAILS_DATA'])
        return df

    def select_1st_n_days(self, df: DataFrame, n_days: int) -> DataFrame:
        columns_of_relevance = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]\
                               + [f'd_{i}' for i in range(1, n_days+1)]
        df_sales_details_100days = df.select(columns_of_relevance)
        return df_sales_details_100days

    def unpivot_day_wise_sales_data(self, df: DataFrame, n_days: int) -> DataFrame:
        l1 = [f"d_{i}" for i in range(1, n_days+1)]
        unpivotExpr = f"stack({n_days}"
        for i in l1:
            unpivotExpr += f", '{i}'" + ", "
            unpivotExpr += i
        unpivotExpr += ") as (day, units_sold)"
        df_sales_details_100days_unpivotted = df.selectExpr("id", "item_id", "dept_id", "cat_id",
                                                            "store_id", "state_id", unpivotExpr)\
                                                .where("units_sold is not null")
        return df_sales_details_100days_unpivotted

    def get_master_data(self, df_sales_details_100days_unpivotted: DataFrame,
                              df_sales_price: DataFrame, df_calendar: DataFrame) -> DataFrame:

        df_master = df_sales_details_100days_unpivotted.join(df_sales_price, ["store_id", "item_id"], "inner")\
                                                       .join(df_calendar, ['day', 'wm_yr_wk'], "inner")
        return df_master

    def get_price_earned_per_prod_per_store(self, df_master: DataFrame) -> DataFrame:
        df_master_revenue = df_master.withColumn("revenue", self.psf.col("sell_price") * self.psf.col("units_sold"))
        return df_master_revenue

    def get_top_n_stores(self, df_master_revenue: DataFrame, n_stores: int) -> DataFrame:
        df_top5_stores = df_master_revenue.groupBy('store_id').agg(self.psf.sum('revenue').alias('total_revenue'))\
            .orderBy(self.psf.col('total_revenue').desc()).limit(n_stores)
        return df_top5_stores

    def get_total_units_of_each_prod_per_store(self, df_master_revenue: DataFrame) -> DataFrame:
        df_total_units_sold_per_store_per_prod = df_master_revenue.groupBy("store_id", "item_id")\
                                                .agg(self.psf.sum("units_sold").alias('total_units_sold'))
        return df_total_units_sold_per_store_per_prod

    def get_2_best_selling_prods_per_store(self, df_total_units_sold_per_store_per_prod: DataFrame) -> DataFrame:

        windowSpec = Window.partitionBy("store_id").orderBy(self.psf.col("total_units_sold").desc())
        df_product_sold_rank_per_store = \
            df_total_units_sold_per_store_per_prod.withColumn("store_wise_product_rank", self.psf.rank().over(windowSpec))

        df_store_wise_top2_items = df_product_sold_rank_per_store.filter(self.psf.col("store_wise_product_rank") <= 2)

        df_store_wise_top2_items = df_store_wise_top2_items.groupBy('store_id').agg(
            self.psf.collect_list('item_id').alias('items'))

        df_store_wise_top2_items = df_store_wise_top2_items.withColumn('TOP_PRODUCT_1', self.psf.col('items').getItem(0)) \
            .withColumn('TOP_PRODUCT_2', self.psf.col('items').getItem(1)).drop('items')
        return df_store_wise_top2_items
