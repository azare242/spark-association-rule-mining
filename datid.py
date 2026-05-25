from __future__ import annotations

from typing import Sequence, Set

from pyspark.rdd import RDD

from arm_common import (
    Item,
    Itemset,
    build_frequent_itemset_arg_parser,
    candidates_in_transaction,
    generate_candidates,
    min_support_count,
    normalize_transaction,
    run_frequent_itemset_cli,
)


def sort_by_l1_order(transaction: Itemset, frequent_items: Set[Item], item_order: dict[Item, int]) -> Itemset:
    filtered = [item for item in transaction if item in frequent_items]
    return tuple(sorted(filtered, key=lambda item: (item_order[item], item)))


def shrink_transaction(transaction: Itemset, frequent_itemsets: Sequence[Itemset], next_k: int) -> Itemset:
    contributing_items = {item for itemset in frequent_itemsets for item in itemset}
    if not contributing_items:
        return tuple()
    shrunk = tuple(item for item in transaction if item in contributing_items)
    if len(shrunk) < next_k:
        return tuple()
    return shrunk


def datid(transactions: RDD, min_support: float) -> RDD:
    """
    Distributed Apriori-TID frequent itemset mining.

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

    item_counts = (
        normalized.flatMap(lambda transaction: ((item, 1) for item in transaction))
        .reduceByKey(lambda left, right: left + right)
    )

    l1_counts = (
        item_counts.filter(lambda item_count: item_count[1] >= threshold)
        .map(lambda item_count: ((item_count[0],), item_count[1]))
        .sortBy(lambda item_count: item_count[0])
        .cache()
    )

    frequent_parts = [l1_counts]
    l1_local = l1_counts.collect()
    current_itemsets = [itemset for itemset, _ in l1_local]

    if not current_itemsets:
        normalized.unpersist()
        l1_counts.unpersist()
        return sc.emptyRDD()

    ordered_l1 = sorted(
        ((itemset[0], count) for itemset, count in l1_local),
        key=lambda item_count: (-item_count[1], item_count[0]),
    )
    frequent_items = {item for item, _ in ordered_l1}
    item_order = {item: index for index, (item, _) in enumerate(ordered_l1)}

    frequent_items_broadcast = sc.broadcast(frequent_items)
    item_order_broadcast = sc.broadcast(item_order)
    array_data = (
        normalized.map(
            lambda transaction: sort_by_l1_order(
                transaction,
                frequent_items_broadcast.value,
                item_order_broadcast.value,
            )
        )
        .filter(lambda transaction: len(transaction) > 1)
        .cache()
    )
    array_data.count()

    normalized.unpersist()
    frequent_items_broadcast.unpersist()
    item_order_broadcast.unpersist()

    k = 2
    while len(current_itemsets) > 1:
        candidates = generate_candidates(current_itemsets, k)
        if not candidates:
            break

        candidates_broadcast = sc.broadcast(candidates)
        next_frequents = (
            array_data.flatMap(
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

        previous_array_data = array_data
        current_itemsets_broadcast = sc.broadcast(current_itemsets)
        array_data = (
            previous_array_data.map(
                lambda transaction: shrink_transaction(
                    transaction,
                    current_itemsets_broadcast.value,
                    k + 1,
                )
            )
            .filter(lambda transaction: len(transaction) > 0)
            .cache()
        )
        array_data.count()
        previous_array_data.unpersist()
        current_itemsets_broadcast.unpersist()

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
    parser = build_frequent_itemset_arg_parser("Run DATID with PySpark.", "DATID")
    run_frequent_itemset_cli(parser, datid)


if __name__ == "__main__":
    main()
