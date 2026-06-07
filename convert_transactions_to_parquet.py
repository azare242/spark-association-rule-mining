from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transactions_text_to_parquet(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    delimiter: str = ",",
    overwrite: bool = False,
) -> None:
    """
    Convert a text transaction file into Parquet for spark_arm_stream.py.

    Input format:
        item_1,item_2,item_3
        item_2,item_4

    Output schema:
        transaction_id: long
        raw_line: string
        items: array<string>
        item_count: long
    """
    if delimiter == "whitespace":
        split_expr = r"split(trim(raw_line), '\\s+')"
    else:
        escaped_delimiter = delimiter.replace("\\", "\\\\").replace("'", "\\'")
        split_expr = f"split(raw_line, '{escaped_delimiter}')"

    transactions = (
        spark.read.text(input_path)
        .where(F.length(F.trim(F.col("value"))) > 0)
        .withColumn("transaction_id", F.monotonically_increasing_id())
        .withColumn("raw_line", F.col("value"))
        .withColumn(
            "items",
            F.expr(f"filter(transform({split_expr}, x -> trim(x)), x -> x != '')"),
        )
        .withColumn("item_count", F.size(F.col("items")).cast("long"))
        .select("transaction_id", "raw_line", "items", "item_count")
    )

    write_mode = "overwrite" if overwrite else "error"
    transactions.write.mode(write_mode).parquet(output_path)
    print(f"Wrote {transactions.count():,} transactions to {output_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert a text transaction file into Parquet for Spark streaming."
    )
    parser.add_argument("input", help="Input text file. Each line is one transaction.")
    parser.add_argument(
        "--output",
        default="otto_transactions_parquet",
        help="Output Parquet directory. Default: otto_transactions_parquet.",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help='Item delimiter. Use "whitespace" to split on spaces/tabs. Default: comma.',
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the output directory if it already exists.",
    )
    parser.add_argument("--app-name", default="ConvertTransactionsToParquet")
    parser.add_argument("--master", default=None, help="Optional Spark master, e.g. local[*].")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()

    builder = SparkSession.builder.appName(args.app_name)
    if args.master:
        builder = builder.master(args.master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        transactions_text_to_parquet(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            delimiter=args.delimiter,
            overwrite=args.overwrite,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
