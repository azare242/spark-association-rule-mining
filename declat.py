from __future__ import annotations

import argparse
import itertools
import math
from typing import Iterable, List, Sequence, Set, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD


Item = str
Itemset = Tuple[Item, ...]
TidSet = Tuple[int, ...]
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


def intersect_tidsets(left: TidSet, right: TidSet) -> TidSet:
    left_set = set(left)
    return tuple(tid for tid in right if tid in left_set)


def candidate_prefix_pairs(candidate: Itemset) -> Tuple[Tuple[Itemset, Tuple[Itemset, Item]], Tuple[Itemset, Tuple[Itemset, Item]]]:
    left_parent = candidate[:-1]
    right_parent = candidate[:-2] + candidate[-1:]
    return (
        (left_parent, (candidate, "left")),
        (right_parent, (candidate, "right")),
    )


def build_candidate_tidsets(
    current_vertical: RDD,
    candidates: Sequence[Itemset],
) -> RDD:
    sc = current_vertical.context
    candidate_parent_pairs = sc.parallelize(candidates).flatMap(candidate_prefix_pairs)

    joined = candidate_parent_pairs.join(current_vertical)
    grouped = joined.map(
        lambda row: (row[1][0][0], (row[1][0][1], row[1][1]))
    ).groupByKey()

    return grouped.flatMap(intersect_candidate_parents)


def intersect_candidate_parents(grouped_row: Tuple[Itemset, Iterable[Tuple[str, TidSet]]]) -> Iterable[Tuple[Itemset, TidSet]]:
    candidate, parent_tidsets = grouped_row
    parent_map = {side: tidset for side, tidset in parent_tidsets}
    left = parent_map.get("left")
    right = parent_map.get("right")
    if left is None or right is None:
        return []
    return [(candidate, intersect_tidsets(left, right))]


def declat(transactions: RDD, min_support: float) -> RDD:
    """
    Distributed ECLAT frequent itemset mining.

    Args:
        transactions: RDD whose records are iterables of transaction items.
        min_support: Support threshold. Values in (0, 1] are treated as a
            fraction of all transactions; values greater than 1 are treated as
            an absolute support count.

    Returns:
        RDD of (itemset_tuple, support_count, support_fraction).
    """
    min_support = float(min_support)
    if min_support <= 0:
        raise ValueError("min_support must be greater than 0")

    sc = transactions.context
    normalized = transactions.map(normalize_transaction).filter(lambda t: len(t) > 0).cache()
    transaction_count = normalized.count()

    if transaction_count == 0:
        normalized.unpersist()
        return sc.emptyRDD()

    threshold = min_support_count(min_support, transaction_count)

    transactions_with_tids = normalized.zipWithIndex().map(lambda row: (int(row[1]), row[0])).cache()
    normalized.unpersist()

    vertical_data = (
        transactions_with_tids.flatMap(
            lambda tid_transaction: (
                ((item,), tid_transaction[0])
                for item in tid_transaction[1]
            )
        )
        .groupByKey()
        .mapValues(lambda tids: tuple(sorted(tids)))
        .cache()
    )
    vertical_data.count()

    transactions_with_tids.unpersist()

    current_vertical = (
        vertical_data.filter(lambda item_tidset: len(item_tidset[1]) >= threshold)
        .sortBy(lambda item_tidset: item_tidset[0])
        .cache()
    )

    current_itemsets = [itemset for itemset, _ in current_vertical.collect()]
    vertical_data.unpersist()

    if not current_itemsets:
        current_vertical.unpersist()
        return sc.emptyRDD()

    current_counts = current_vertical.mapValues(len).cache()
    current_counts.count()
    frequent_parts = [current_counts]
    k = 2

    while len(current_itemsets) > 1:
        candidates = generate_candidates(current_itemsets, k)
        if not candidates:
            break

        next_vertical = (
            build_candidate_tidsets(current_vertical, candidates)
            .filter(lambda item_tidset: len(item_tidset[1]) >= threshold)
            .sortBy(lambda item_tidset: item_tidset[0])
            .cache()
        )

        next_itemsets = [itemset for itemset, _ in next_vertical.collect()]

        if not next_itemsets:
            next_vertical.unpersist()
            break

        next_counts = next_vertical.mapValues(len).cache()
        next_counts.count()
        frequent_parts.append(next_counts)
        previous_vertical = current_vertical
        current_vertical = next_vertical
        previous_vertical.unpersist()
        current_itemsets = next_itemsets
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
    parser = argparse.ArgumentParser(description="Run DECLAT with PySpark.")
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
    parser.add_argument("--app-name", default="DECLAT")
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
        frequent_itemsets = declat(transactions, args.min_support)

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
