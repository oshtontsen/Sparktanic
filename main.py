from ingest import ingest
from utilities.spark import initialise_spark


def main() -> None:
    spark = initialise_spark()
    import pdb; pdb.set_trace()


if __name__ == "__main__":
    main()
