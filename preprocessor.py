from pyspark.sql.functions import isnan, when, count, col


class Preprocessor:
    def __init__(self, train_df, test_df):
        self.train_df = train_df
        self.test_df = test_df
        self.col_types = {}
        self.nan_cols = None

    def get_column_types(self, df):
        """Get an overview of the data column types"""
        for i in df.dtypes:
            self.col_types[i[0]] = self.col_types.get(i[0], i[1])

    def get_nan_cols(self, df):
        """Return columns with nan"""
        return df.select([count(when(isnan(c), c)).alias(c) for c in df.columns])

    def preprocess_data(self, df):
        """Preprocess the data for training and inference"""
        self.get_column_types(df)
        self.nan_cols = self.get_nan_cols(df)
        return


if __name__ == "__main__":
    pass
