from __future__ import annotations

import argparse
import itertools
import math
from typing import Iterable, List, Sequence, Set, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD


Item = str
Itemset = Tuple[Item, ...]
FrequentItemset = Tuple[Itemset, int, float]


def normalize_transaction(transaction: Iterable[object]) -> Itemset:
    """Return a sorted tuple of unique string items from one transaction."""
    return tuple(sorted({str(item).strip() for item in transaction if str(item).strip()}))


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


def candidates_in_transaction(transaction: Itemset, candidates: Sequence[Itemset]) -> Iterable[Tuple[Itemset, int]]:
    transaction_items = set(transaction)
    for candidate in candidates:
        if set(candidate).issubset(transaction_items):
            yield candidate, 1


def dapriori(transactions: RDD, min_support: float) -> RDD:
    """
    Distributed Apriori frequent itemset mining.

    Args:
        transactions: RDD whose records are iterables of transaction items.
        min_support: Support threshold. Values in (0, 1] are treated as a
            fraction of all transactions; values greater than 1 are treated as
            an absolute support count.

    Returns:
        RDD of (itemset_tuple, support_count, support_fraction).
    """
    sc = transactions.context
    normalized = transactions.map(normalize_transaction).filter(lambda t: len(t) > 0).cache()
    transaction_count = normalized.count()

    if transaction_count == 0:
        return sc.emptyRDD()

    threshold = min_support_count(float(min_support), transaction_count)
    if threshold <= 0:
        raise ValueError("min_support must be greater than 0")

    item_counts = (
        normalized.flatMap(lambda transaction: ((item, 1) for item in transaction))
        .reduceByKey(lambda left, right: left + right)
    )

    current = (
        item_counts.filter(lambda item_count: item_count[1] >= threshold)
        .map(lambda item_count: ((item_count[0],), item_count[1]))
        .sortBy(lambda item_count: item_count[0])
        .cache()
    )

    frequent_parts = [current]
    current_itemsets = [itemset for itemset, _ in current.collect()]
    k = 2

    while len(current_itemsets) > 1:
        candidates = generate_candidates(current_itemsets, k)
        if not candidates:
            break

        candidates_broadcast = sc.broadcast(candidates)
        next_frequents = (
            normalized.flatMap(
                lambda transaction: candidates_in_transaction(
                    transaction,
                    candidates_broadcast.value,
                )
            )
            .reduceByKey(lambda left, right: left + right)
            .filter(lambda item_count: item_count[1] >= threshold)
            .sortBy(lambda item_count: item_count[0])
            .cache()
        )

        current_itemsets = [itemset for itemset, _ in next_frequents.collect()]
        candidates_broadcast.unpersist()

        if not current_itemsets:
            next_frequents.unpersist()
            break

        frequent_parts.append(next_frequents)
        current = next_frequents
        k += 1

    result = sc.union(frequent_parts).map(
        lambda item_count: (
            item_count[0],
            item_count[1],
            item_count[1] / float(transaction_count),
        )
    )

    return result.sortBy(lambda row: (len(row[0]), row[0]))


def parse_transaction_line(line: str, delimiter: str) -> Itemset:
    if delimiter == "whitespace":
        return normalize_transaction(line.split())
    return normalize_transaction(line.split(delimiter))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DApriori with PySpark.")
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
    parser.add_argument("--app-name", default="DApriori")
    parser.add_argument("--master", default=None, help="Optional Spark master, e.g. local[*].")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()

    conf = SparkConf().setAppName(args.app_name)
    if args.master:
        conf = conf.setMaster(args.master)

    sc = SparkContext(conf=conf)
    try:
        raw = sc.textFile(args.input)
        transactions = raw.map(lambda line: parse_transaction_line(line, args.delimiter))
        frequent_itemsets = dapriori(transactions, args.min_support)

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


if __name__ == "__main__":
    main()
