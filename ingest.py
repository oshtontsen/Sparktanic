import constants as cst

from utilities.spark import get_current_session


class Ingest:
    @staticmethod
    def ingest_data():
        """Read train and test from csv"""
        session = get_current_session()
        train_df = session.read.format("csv").option("header", "true").load(cst.TRAIN_DATA_PATH)
        test_df = session.read.format("csv").option("header", "true").load(cst.TEST_DATA_PATH)
        return train_df, test_df


if __name__ == "__main__":
    pass
