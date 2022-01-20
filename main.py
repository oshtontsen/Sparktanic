import constants as cst

from ingest import Ingest
from preprocessor import Preprocessor
from utilities.spark import initialise_spark


def main() -> None:
    spark = initialise_spark()
    train_df, test_df = Ingest.ingest_data()
    Preprocessor.preprocess_data(train_df)
    Preprocessor.preprocess_data(test_df)
    return train_df, test_df


if __name__ == "__main__":
    main()
