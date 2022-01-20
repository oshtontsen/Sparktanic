import constants as cst

from ingest import ingest
from utilities.spark import initialise_spark


def main() -> None:
    spark = initialise_spark()
    train_df, test_df = ingest()
    import pdb; pdb.set_trace()

    # Combine the train and test data and preprocess together
    combined_data = train_df.union(test_df)

    return train_data


if __name__ == "__main__":
    main()
