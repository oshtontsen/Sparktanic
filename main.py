import constants as cst

from ingest import ingest
from utilities.spark import initialise_spark


def main() -> None:
    spark = initialise_spark()
    df = spark.read.format("csv").option("header", "true").load(cst.DATA_PATH)
    return df


if __name__ == "__main__":
    main()
