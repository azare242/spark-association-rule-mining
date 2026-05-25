from __future__ import annotations

from typing import Iterable, Sequence, Tuple

from pyspark.rdd import RDD

from arm_common import (
    Item,
    Itemset,
    build_frequent_itemset_arg_parser,
    generate_candidates,
    min_support_count,
    normalize_transaction,
    run_frequent_itemset_cli,
)


TidSet = Tuple[int, ...]


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


def main() -> None:
    parser = build_frequent_itemset_arg_parser("Run DECLAT with PySpark.", "DECLAT")
    run_frequent_itemset_cli(parser, declat)


if __name__ == "__main__":
    main()
