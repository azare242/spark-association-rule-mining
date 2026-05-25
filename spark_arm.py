from __future__ import annotations

import argparse
import itertools
from typing import Iterable, Tuple

from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD

from arm_common import FrequentItemset, Itemset, normalize_itemset, parse_transaction_line
from dapriori import dapriori
from datid import datid
from declat import declat


AssociationRule = Tuple[Itemset, Itemset, float, int, float]


def non_empty_proper_subsets(itemset: Itemset) -> Iterable[Itemset]:
    for size in range(1, len(itemset)):
        for subset in itertools.combinations(itemset, size):
            yield tuple(subset)


def candidate_rules_from_itemset(
    frequent_itemset: FrequentItemset,
    support_by_itemset: dict[Itemset, float],
) -> Iterable[AssociationRule]:
    itemset, support_count, support_fraction = frequent_itemset
    normalized_itemset = normalize_itemset(itemset)

    if len(normalized_itemset) < 2:
        return []

    rules = []
    itemset_items = set(normalized_itemset)
    for antecedent in non_empty_proper_subsets(normalized_itemset):
        antecedent_support = support_by_itemset.get(antecedent)
        if antecedent_support is None or antecedent_support <= 0:
            continue

        consequent = tuple(item for item in normalized_itemset if item not in set(antecedent))
        if not consequent:
            continue

        confidence = support_fraction / antecedent_support
        if itemset_items == set(antecedent) | set(consequent):
            rules.append(
                (
                    antecedent,
                    consequent,
                    confidence,
                    support_count,
                    support_fraction,
                )
            )

    return rules


def spark_association_rule_mining(frequent_itemsets: RDD, min_confidence: float) -> RDD:
    """
    Generate association rules from frequent itemsets with Spark.

    Args:
        frequent_itemsets: RDD of (itemset_tuple, support_count, support_fraction),
            matching the output of dapriori(), datid(), and declat().
        min_confidence: Minimum confidence threshold in [0, 1].

    Returns:
        RDD of (antecedent_tuple, consequent_tuple, confidence,
        support_count, support_fraction).
    """
    min_confidence = float(min_confidence)
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")

    sc = frequent_itemsets.context
    normalized_frequents = frequent_itemsets.map(
        lambda row: (
            normalize_itemset(row[0]),
            int(row[1]),
            float(row[2]),
        )
    ).cache()

    support_by_itemset = dict(
        normalized_frequents.map(lambda row: (row[0], row[2])).collect()
    )
    if not support_by_itemset:
        return sc.emptyRDD()

    support_broadcast = sc.broadcast(support_by_itemset)
    rules = (
        normalized_frequents.flatMap(
            lambda row: candidate_rules_from_itemset(row, support_broadcast.value)
        )
        .filter(lambda rule: rule[2] >= min_confidence)
        .sortBy(lambda rule: (len(rule[0]) + len(rule[1]), rule[0], rule[1]))
    )

    support_broadcast.unpersist()
    return rules


def frequent_itemsets_from_transactions(
    transactions: RDD,
    min_support: float,
    algorithm: str,
) -> RDD:
    algorithm_name = algorithm.lower()
    if algorithm_name == "dapriori":
        return dapriori(transactions, min_support)
    if algorithm_name == "datid":
        return datid(transactions, min_support)
    if algorithm_name == "declat":
        return declat(transactions, min_support)
    raise ValueError(f"Unsupported frequent itemset algorithm: {algorithm}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Spark Association Rule Mining with PySpark.")
    parser.add_argument("input", help="Input text file. Each line is one transaction.")
    parser.add_argument(
        "--min-support",
        type=float,
        required=True,
        help="Minimum support for frequent itemset mining. Use 0 < value <= 1 for fraction, or > 1 for count.",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        required=True,
        help="Minimum confidence threshold in [0, 1].",
    )
    parser.add_argument(
        "--algorithm",
        choices=("dapriori", "datid", "declat"),
        default="dapriori",
        help="Frequent itemset algorithm used before rule generation. Default: dapriori.",
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
    parser.add_argument("--app-name", default="SparkAssociationRuleMining")
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
        frequent_itemsets = frequent_itemsets_from_transactions(
            transactions,
            args.min_support,
            args.algorithm,
        )
        rules = spark_association_rule_mining(frequent_itemsets, args.min_confidence)

        formatted = rules.map(
            lambda rule: (
                f"{','.join(rule[0])} -> {','.join(rule[1])}"
                f"\tconfidence={rule[2]:.6f}"
                f"\tsupport_count={rule[3]}"
                f"\tsupport={rule[4]:.6f}"
            )
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
