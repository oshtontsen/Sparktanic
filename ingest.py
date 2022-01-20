import constants as cst

from utilities.spark import get_current_session


def ingest():
    session = get_current_session()
    # session = SparkSession.builder.getOrCreate()
    train_data = session.read.format("csv").option("header", "true").load(cst.DATA_PATH)
    test_data = session.read.format("csv").option("header", "true").load(cst.DATA_PATH)
    return train_data, test_data


def main():
    pass


if __name__ == "__main__":
    main()
