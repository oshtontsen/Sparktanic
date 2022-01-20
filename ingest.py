import constants as cst

from utilities.spark import get_current_session


def ingest():
    """Read train and test from csv"""
    session = get_current_session()
    # session = SparkSession.builder.getOrCreate()
    train_df = session.read.format("csv").option("header", "true").load(cst.TRAIN_DATA_PATH)
    test_df = session.read.format("csv").option("header", "true").load(cst.TEST_DATA_PATH)
    return train_df, test_df


def main():
    pass


if __name__ == "__main__":
    main()
