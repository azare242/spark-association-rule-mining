from __future__ import annotations

import argparse
import itertools
import math
from typing import Callable, Iterable, List, Sequence, Set, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD


Item = str
Itemset = Tuple[Item, ...]
FrequentItemset = Tuple[Itemset, int, float]


def normalize_itemset(itemset: Iterable[object]) -> Itemset:
    """Return a sorted tuple of unique string items."""
    return tuple(sorted({str(item).strip() for item in itemset if str(item).strip()}))


def normalize_transaction(transaction: Iterable[object]) -> Itemset:
    return normalize_itemset(transaction)


def min_support_count(min_support: float, transaction_count: int) -> int:
    if transaction_count <= 0:
        return 0
    if 0 < min_support <= 1:
        return math.ceil(min_support * transaction_count)
    return math.ceil(min_support)


def generate_candidates(previous_frequents: Sequence[Itemset], k: int) -> List[Itemset]:
    previous = sorted(tuple(itemset) for itemset in previous_frequents)
    previous_lookup: Set[Itemset] = set(previous)
    candidates: Set[Itemset] = set()

    for left_index in range(len(previous)):
        for right_index in range(left_index + 1, len(previous)):
            left = previous[left_index]
            right = previous[right_index]

            if left[: k - 2] != right[: k - 2]:
                break

            candidate = tuple(sorted(set(left) | set(right)))
            if len(candidate) != k:
                continue

            all_subsets_frequent = all(
                tuple(subset) in previous_lookup
                for subset in itertools.combinations(candidate, k - 1)
            )
            if all_subsets_frequent:
                candidates.add(candidate)

    return sorted(candidates)


def candidates_in_transaction(
    transaction: Itemset,
    candidates: Sequence[Itemset],
) -> Iterable[Tuple[Itemset, int]]:
    transaction_items = set(transaction)
    for candidate in candidates:
        if set(candidate).issubset(transaction_items):
            yield candidate, 1


def parse_transaction_line(line: str, delimiter: str) -> Itemset:
    if delimiter == "whitespace":
        return normalize_transaction(line.split())
    return normalize_transaction(line.split(delimiter))


def build_frequent_itemset_arg_parser(
    description: str,
    default_app_name: str,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("input", help="Input text file. Each line is one transaction.")
    parser.add_argument(
        "--min-support",
        type=float,
        required=True,
        help="Minimum support. Use 0 < value <= 1 for fraction, or > 1 for count.",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help='Item delimiter. Use "whitespace" to split on spaces/tabs. Default: comma.',
    )
    parser.add_argument(
        "--output",
        help="Optional output directory for Spark text output. Prints to stdout if omitted.",
    )
    parser.add_argument("--app-name", default=default_app_name)
    parser.add_argument("--master", default=None, help="Optional Spark master, e.g. local[*].")
    return parser


def run_frequent_itemset_cli(
    parser: argparse.ArgumentParser,
    algorithm: Callable[[RDD, float], RDD],
) -> None:
    args = parser.parse_args()

    conf = SparkConf().setAppName(args.app_name)
    if args.master:
        conf = conf.setMaster(args.master)

    sc = SparkContext(conf=conf)
    try:
        raw = sc.textFile(args.input)
        transactions = raw.map(lambda line: parse_transaction_line(line, args.delimiter))
        frequent_itemsets = algorithm(transactions, args.min_support)

        formatted = frequent_itemsets.map(
            lambda row: f"{','.join(row[0])}\tcount={row[1]}\tsupport={row[2]:.6f}"
        )
        if args.output:
            formatted.saveAsTextFile(args.output)
        else:
            for line in formatted.collect():
                print(line)
    finally:
        sc.stop()
