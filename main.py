import constants as cst

from ingest import ingest
from utilities.spark import initialise_spark


def main() -> None:
    spark = initialise_spark()
    train_df, test_df = ingest()
    return train_df, test_df


if __name__ == "__main__":
    main()
