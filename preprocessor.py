from pyspark.sql.functions import isnan, when, count, col


class Preprocessor:
    def __init__(self, train_df, test_df):
        self.train_df = train_df
        self.test_df = test_df
        self.col_types = {}
        self.nan_cols = None
        self.null_cols = None

    def get_column_types(self, df):
        """Get an overview of the data column types"""
        for i in df.dtypes:
            self.col_types[i[0]] = self.col_types.get(i[0], i[1])

    def get_nan_cols(self, df):
        """Return columns with nan"""
        return df.select([count(when(isnan(c), c)).alias(c) for c in df.columns])

    def get_null_cols(self, df):
        """Return columns with null"""
        return df.select([count(when(df[c].isNull(), c)).alias(c) for c in df.columns])

    def preprocess_age(self, df):
        """Impute the mean value for age. Note that Pyspark mean is more
        accurate than median due to its distributed manner."""
        from pyspark.sql.functions import (
            isnan,
            lit,
            when,
            count,
            col,
            percentile_approx,
            expr,
        )

        # NOTE: Convert to float instead of double because double requires
        # additional storage space and should only be used for large numbers
        # Get rows with null Age
        # df.where(col("Age").isNull()).show()
        # ----
        # METHOD 1
        # ----
        # from pyspark.ml.feature import Imputer
        # from pyspark.sql.types import FloatType
        # df = df.withColumn("Age", df["Age"].cast(FloatType()))
        # imputer = Imputer(inputCols=["Age"], outputCols=["Age_Imputed"])
        # model = imputer.fit(df)
        # df = model.transform(df)
        # ----

        # ----
        # METHOD 2: w/ GroupBy
        # This method will replace existing ages rather than just impute missing values
        # ----
        # from pyspark.sql import Window

        # grp_window = Window.partitionBy('Sex', 'Pclass')
        # magic_percentile = expr('percentile_approx(Age, 0.5)')

        # df = df.withColumn('Age_imputed', magic_percentile.over(grp_window))
        # df.select('Age_imputed').distinct().show()
        # ----

        # ----
        # METHOD 3
        # This method does not do the imputations, maybe try use coalesce() or union()
        # https://stackoverflow.com/questions/46845672/median-quantiles-within-pyspark-groupby
        # ----
        from pyspark.sql.types import FloatType

        df = df.withColumn("Age", df["Age"].cast(FloatType()))
        df_ = df.groupBy("Sex", "Pclass").agg(
            percentile_approx("Age", 0.5, lit(1000000)).alias("Age_imputed")
        )
        # ----

    def preprocess_data(self, df):
        """Preprocess the data for training and inference"""
        self.get_column_types(df)
        self.nan_cols = self.get_nan_cols(df)
        self.null_cols = self.get_null_cols(df)
        self.preprocess_age(df)
        return


if __name__ == "__main__":
    pass
