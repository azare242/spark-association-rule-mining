from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from arm_common import parse_transaction_line
from dapriori import dapriori
from datid import datid
from declat import declat
from spark_arm import frequent_itemsets_from_transactions, spark_association_rule_mining


FREQUENT_ITEMSET_ALGORITHMS = {
    "dapriori": dapriori,
    "datid": datid,
    "declat": declat,
}

PARQUET_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", LongType(), nullable=True),
        StructField("raw_line", StringType(), nullable=True),
        StructField("items", ArrayType(StringType(), containsNull=False), nullable=True),
        StructField("item_count", LongType(), nullable=True),
    ]
)


def transactions_rdd_from_batch(batch_df: DataFrame, delimiter: str):
    """
    Convert one Structured Streaming micro-batch into the transaction RDD shape
    expected by dapriori(), datid(), and declat().
    """
    columns = set(batch_df.columns)
    if "items" in columns:
        return (
            batch_df.select("items")
            .rdd.map(lambda row: tuple(item for item in (row["items"] or []) if item))
            .filter(lambda transaction: len(transaction) > 0)
        )
    if "raw_line" in columns:
        return (
            batch_df.select("raw_line")
            .rdd.map(lambda row: parse_transaction_line(row["raw_line"] or "", delimiter))
            .filter(lambda transaction: len(transaction) > 0)
        )
    if "value" in columns:
        return (
            batch_df.select("value")
            .rdd.map(lambda row: parse_transaction_line(row["value"] or "", delimiter))
            .filter(lambda transaction: len(transaction) > 0)
        )

    raise ValueError(
        "Streaming batch must contain one of these columns: items, raw_line, value"
    )


def dapriori_stream(batch_df: DataFrame, min_support: float, delimiter: str = ","):
    transactions = transactions_rdd_from_batch(batch_df, delimiter)
    return dapriori(transactions, min_support)


def datid_stream(batch_df: DataFrame, min_support: float, delimiter: str = ","):
    transactions = transactions_rdd_from_batch(batch_df, delimiter)
    return datid(transactions, min_support)


def declat_stream(batch_df: DataFrame, min_support: float, delimiter: str = ","):
    transactions = transactions_rdd_from_batch(batch_df, delimiter)
    return declat(transactions, min_support)


def frequent_itemsets_stream(
    batch_df: DataFrame,
    min_support: float,
    algorithm: str,
    delimiter: str = ",",
):
    transactions = transactions_rdd_from_batch(batch_df, delimiter)
    return frequent_itemsets_from_transactions(transactions, min_support, algorithm)


def association_rules_stream(
    batch_df: DataFrame,
    min_support: float,
    min_confidence: float,
    algorithm: str,
    delimiter: str = ",",
):
    frequent_itemsets = frequent_itemsets_stream(
        batch_df=batch_df,
        min_support=min_support,
        algorithm=algorithm,
        delimiter=delimiter,
    )
    return spark_association_rule_mining(frequent_itemsets, min_confidence)


def format_frequent_itemset(row) -> str:
    return f"{','.join(row[0])}\tcount={row[1]}\tsupport={row[2]:.6f}"


def format_association_rule(rule) -> str:
    return (
        f"{','.join(rule[0])} -> {','.join(rule[1])}"
        f"\tconfidence={rule[2]:.6f}"
        f"\tsupport_count={rule[3]}"
        f"\tsupport={rule[4]:.6f}"
    )


def write_or_print_rdd(rdd, formatter: Callable, output: Optional[str], batch_id: int) -> None:
    formatted = rdd.map(formatter)
    if output:
        batch_output = str(Path(output) / f"batch_id={batch_id}")
        formatted.saveAsTextFile(batch_output)
        print(f"batch_id={batch_id}: wrote results to {batch_output}")
        return

    rows = formatted.collect()
    print(f"\n===== batch_id={batch_id}, rows={len(rows)} =====")
    for line in rows:
        print(line)


def is_empty_batch(batch_df: DataFrame) -> bool:
    return batch_df.limit(1).count() == 0


def process_frequent_itemsets_batch(
    batch_df: DataFrame,
    batch_id: int,
    min_support: float,
    algorithm: str,
    delimiter: str,
    output: Optional[str],
) -> None:
    if is_empty_batch(batch_df):
        print(f"batch_id={batch_id}: empty batch")
        return

    frequent_itemsets = frequent_itemsets_stream(
        batch_df=batch_df,
        min_support=min_support,
        algorithm=algorithm,
        delimiter=delimiter,
    )
    write_or_print_rdd(frequent_itemsets, format_frequent_itemset, output, batch_id)


def process_association_rules_batch(
    batch_df: DataFrame,
    batch_id: int,
    min_support: float,
    min_confidence: float,
    algorithm: str,
    delimiter: str,
    output: Optional[str],
) -> None:
    if is_empty_batch(batch_df):
        print(f"batch_id={batch_id}: empty batch")
        return

    rules = association_rules_stream(
        batch_df=batch_df,
        min_support=min_support,
        min_confidence=min_confidence,
        algorithm=algorithm,
        delimiter=delimiter,
    )
    write_or_print_rdd(rules, format_association_rule, output, batch_id)


def build_stream_reader(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.source_format == "text":
        return spark.readStream.text(args.input)
    if args.source_format == "parquet":
        return spark.readStream.schema(PARQUET_TRANSACTION_SCHEMA).parquet(args.input)
    raise ValueError(f"Unsupported source format: {args.source_format}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run streaming frequent itemset or association rule mining with PySpark."
    )
    parser.add_argument("input", help="Input stream directory or file path.")
    parser.add_argument(
        "--source-format",
        choices=("text", "parquet"),
        default="text",
        help="Input stream format. Text uses a 'value' column; Parquet expects an 'items' array column.",
    )
    parser.add_argument(
        "--mode",
        choices=("frequent-itemsets", "rules"),
        default="frequent-itemsets",
        help="Stream output mode. Default: frequent-itemsets.",
    )
    parser.add_argument(
        "--algorithm",
        choices=tuple(FREQUENT_ITEMSET_ALGORITHMS),
        default="dapriori",
        help="Frequent itemset algorithm used per micro-batch. Default: dapriori.",
    )
    parser.add_argument(
        "--min-support",
        type=float,
        required=True,
        help="Minimum support. Use 0 < value <= 1 for micro-batch fraction, or > 1 for count.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=None,
        help="Minimum confidence in [0, 1]. Required when --mode rules.",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help='Text item delimiter. Use "whitespace" to split on spaces/tabs. Default: comma.',
    )
    parser.add_argument(
        "--checkpoint",
        default="checkpoints/spark_arm_stream",
        help="Structured Streaming checkpoint directory.",
    )
    parser.add_argument(
        "--output",
        help="Optional output directory. Results are written under batch_id=<id> subdirectories.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Keep the stream running. By default Spark processes currently available files and exits.",
    )
    parser.add_argument("--app-name", default="SparkAssociationRuleMiningStream")
    parser.add_argument("--master", default=None, help="Optional Spark master, e.g. local[*].")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.min_support <= 0:
        raise ValueError("min_support must be greater than 0")
    if args.mode == "rules" and args.min_confidence is None:
        raise ValueError("--min-confidence is required when --mode rules")
    if args.min_confidence is not None and not 0 <= args.min_confidence <= 1:
        raise ValueError("min_confidence must be between 0 and 1")


def main() -> None:
    args = build_arg_parser().parse_args()
    validate_args(args)

    builder = SparkSession.builder.appName(args.app_name)
    if args.master:
        builder = builder.master(args.master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        stream_df = build_stream_reader(spark, args)

        if args.mode == "frequent-itemsets":
            foreach_batch = lambda batch_df, batch_id: process_frequent_itemsets_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                min_support=args.min_support,
                algorithm=args.algorithm,
                delimiter=args.delimiter,
                output=args.output,
            )
        else:
            foreach_batch = lambda batch_df, batch_id: process_association_rules_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                min_support=args.min_support,
                min_confidence=args.min_confidence,
                algorithm=args.algorithm,
                delimiter=args.delimiter,
                output=args.output,
            )

        query_builder = (
            stream_df.writeStream.foreachBatch(foreach_batch)
            .option("checkpointLocation", args.checkpoint)
        )
        if not args.continuous:
            query_builder = query_builder.trigger(availableNow=True)

        query = query_builder.start()
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
