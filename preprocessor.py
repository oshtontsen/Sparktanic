from pyspark.sql.functions import isnan, when, count, col, split, regexp_extract


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
        # TODO: Look into expr() and regexp_replace() as seen in #5 of https://sparkbyexamples.com/pyspark/pyspark-replace-column-values/
        # ----

    def preprocess_embarked(self, df):
        """Preproccess the embark categorical feature"""
        # Check where high-class women embarked from
        # df.where((df.Pclass == '1') & (df.Sex == 'female')).select('Sex', 'Pclass', 'Embarked').count()
        # Impute google'ed value for Stone, Mrs. George Nelson (Martha Evelyn) and Amelie Icard
        return df.fillna("S", subset=["Embarked"])

    def preprocess_fare(self, df):
        """Preprocess passenger fares"""
        # NOTE: There are no passengers missing fare in the train data
        # Median Fare value of a male with a third class ticket and no family is a logical choice to fill the missing value.
        # df.where(isnan(col('Fare'))).count()
        # df.where(col('Fare').isNull()).count()
        # from pyspark.sql.functions import lit, percentile_approx
        # df_fare = df.groupBy('Sex', 'Pclass', 'SibSp', 'Parch').agg(percentile_approx("Fare", 0.5, lit(1000000)).alias("Fare_median")).show()
        # med_fare = df_fare.where((col('Sex') == 'male') & (col('Pclass') == 3) & (col('SibSp') == '0') & (col('Parch') == '0')).select('Fare_median').collect()[0][0]
        return df

    def preprocess_cabin(self, df):
        """Preprocess passenger cabin categorical feature using the following rules:
        A, B and C cabins are labeled as ABC because all of them have only 1st class passengers
        D and E cabins are labeled as DE because both of them have similar passenger class distribution and same survival rate
        F and G cabins are labeled as FG because of the same reason above
        M cabin doesn't need to be grouped with other cabins because it is very different from others and has the lowest survival rate.
        """
        # df.where(col('Cabin').isNull()).count()
        df = df.withColumn(
            "Cabin",
            when(df.Cabin.startswith("A"), "ABC")
            .when(df.Cabin.startswith("B"), "ABC")
            .when(df.Cabin.startswith("T"), "ABC")
            .when(df.Cabin.startswith("C"), "ABC")
            .when(df.Cabin.startswith("D"), "DE")
            .when(df.Cabin.startswith("E"), "DE")
            .when(df.Cabin.startswith("F"), "FG")
            .when(df.Cabin.startswith("G"), "FG")
            .otherwise(df.Cabin),
        )
        # M is imputed for null Cabin values
        df = df.fillna("M", subset=["Cabin"])
        return df

    def get_family_size(self, df):
        """Create a new feature for the total family size for a passenger including themselves."""
        df = df.withColumn("Family_Size", col("Parch") + col("SibSp") + int(1))
        # Use int for Family_Size because int is 4 bytes of memory vs double, which is 8 bytes of memory
        return df.withColumn("Family_Size", df.Family_Size.cast('integer'))

    def get_title(self, df):
        """Extract all strings which lie between A-Z or a-z preceding by '.'(dot)""""
        return df.withColumn("Initial", regexp_extract(col("Name"), "([A-Za-z]+)\.", 1))

    def get_surname(self, df):
        """Extract surnames such as Mr., Mrs."""
        return df.withColumn('Surname', regexp_extract(col('Name'), '(\S+),.*', 1))

    def preprocess_data(self, df):
        """Preprocess the data for training and inference"""
        self.get_column_types(df)
        self.nan_cols = self.get_nan_cols(df)
        self.null_cols = self.get_null_cols(df)
        # df = self.preprocess_age(df)
        df = self.preprocess_embarked(df)
        # NOTE: Fare column needs no preprocessing because there are no missing values
        df = self.preprocess_fare(df)
        df = self.preprocess_cabin(df)
        df = self.get_family_size(df)
        df = self.get_title(df)
        df = self.get_surname(df)
        return


if __name__ == "__main__":
    pass
