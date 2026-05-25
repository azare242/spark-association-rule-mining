import argparse
import pandas as pd


DEFAULT_CHUNKSIZE = 50000


def build_arg_parser():
    parser = argparse.ArgumentParser(
        description="Convert Otto train.csv into transaction format for ARM."
    )
    parser.add_argument(
        "input",
        help="Path to Otto train.csv"
    )
    parser.add_argument(
        "--output",
        default="otto_transactions.txt",
        help="Output transaction text file. Default: otto_transactions.txt"
    )
    parser.add_argument(
        "--include-target",
        action="store_true",
        help="Include target_Class_X as an item in each transaction."
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=DEFAULT_CHUNKSIZE,
        help=f"Rows to process per pandas chunk. Default: {DEFAULT_CHUNKSIZE}."
    )
    return parser


def main():
    args = build_arg_parser().parse_args()

    if args.chunksize <= 0:
        raise ValueError("chunksize must be greater than 0")

    header = pd.read_csv(args.input, nrows=0)
    feature_cols = [col for col in header.columns if col.startswith("feat_")]

    with open(args.output, "w", encoding="utf-8") as output:
        for chunk in pd.read_csv(args.input, chunksize=args.chunksize):
            feature_values = chunk[feature_cols].itertuples(index=False, name=None)
            target_values = (
                chunk["target"].astype(str).tolist()
                if args.include_target and "target" in chunk.columns
                else [None] * len(chunk)
            )

            for values, target in zip(feature_values, target_values):
                items = [col for col, value in zip(feature_cols, values) if value > 0]

                if target is not None:
                    items.append("target_" + target)

                output.write(",".join(items) + "\n")


if __name__ == "__main__":
    main()
