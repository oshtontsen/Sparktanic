import constants as cst


def ingest():
    df = pandas.read_csv(cst.DATA_PATH)
    return df


def main():
    pass


if __name__ == "__main__":
    main()
