from __future__ import annotations

from pyspark.rdd import RDD

from arm_common import (
    Itemset,
    build_frequent_itemset_arg_parser,
    candidates_in_transaction,
    generate_candidates,
    min_support_count,
    normalize_transaction,
    run_frequent_itemset_cli,
)


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

    current = (
        item_counts.filter(lambda item_count: item_count[1] >= threshold)
        .map(lambda item_count: ((item_count[0],), item_count[1]))
        .sortBy(lambda item_count: item_count[0])
        .cache()
    )

    frequent_parts = [current]
    current_itemsets = [itemset for itemset, _ in current.collect()]
    if not current_itemsets:
        normalized.unpersist()
        current.unpersist()
        return sc.emptyRDD()

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


def main() -> None:
    parser = build_frequent_itemset_arg_parser("Run DApriori with PySpark.", "DApriori")
    run_frequent_itemset_cli(parser, dapriori)


if __name__ == "__main__":
    main()
