import argparse
import pandas as pd


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
    return parser


def main():
    args = build_arg_parser().parse_args()

    df = pd.read_csv(args.input)
    feature_cols = [col for col in df.columns if col.startswith("feat_")]

    with open(args.output, "w", encoding="utf-8") as output:
        for _, row in df.iterrows():
            items = []

            for col in feature_cols:
                if row[col] > 0:
                    items.append(col)

            if args.include_target and "target" in df.columns:
                items.append("target_" + str(row["target"]))

            output.write(",".join(items) + "\n")


if __name__ == "__main__":
    main()