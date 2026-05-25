# Spark Association Rule Mining

This project contains PySpark implementations for frequent itemset mining and association rule mining.

Implemented files:

| File | Purpose |
|---|---|
| `dapriori.py` | Runs Distributed Apriori and outputs frequent itemsets |
| `datid.py` | Runs Distributed Apriori-TID and outputs frequent itemsets |
| `declat.py` | Runs Distributed ECLAT and outputs frequent itemsets |
| `spark_arm.py` | Runs association rule mining using one of the frequent itemset algorithms |

The input format for all scripts is a plain text transaction file:

```text
item_1,item_2,item_3
item_2,item_4
item_1,item_3,item_5
```

Each line is one transaction, and items are separated by commas by default.

---

## 1. Requirements

Install Java, Spark, and PySpark.

```bash
java -version
spark-submit --version
python --version
```

Install Python dependencies:

```bash
pip install pyspark pandas
```

Recommended Python version: `3.10` or `3.11`.

---

## 2. Prepare the Otto Dataset

The Otto dataset is originally a classification dataset. To use it for Association Rule Mining, each row must be converted into a transaction.

Original Otto format:

```text
id,feat_1,feat_2,...,feat_93,target
1,0,3,...,1,Class_1
```

Transaction format:

```text
feat_2,feat_93,target_Class_1
```

In this conversion:

- Each row becomes one transaction.
- Every feature with value greater than `0` becomes an item.
- The `target` column is optionally added as an item, for example `target_Class_1`.

Run the preparation script:

```bash
python prepare_otto_transactions.py train.csv --output otto_transactions.txt --include-target
```

The script processes the CSV in chunks to avoid loading and iterating the full dataset row-by-row. The default chunk size is `50000` rows. You can change it if your machine needs smaller or larger batches:

```bash
python prepare_otto_transactions.py train.csv \
  --output otto_transactions.txt \
  --include-target \
  --chunksize 25000
```

If you do not want to include the class label in rules:

```bash
python prepare_otto_transactions.py train.csv --output otto_transactions.txt
```

---

## 3. Run Frequent Itemset Mining

All three frequent itemset scripts use the same input arguments:

```bash
spark-submit <algorithm_file.py> <input_file> \
  --min-support <support> \
  --delimiter "," \
  --master local[*]
```

`--min-support` can be used in two ways:

| Value | Meaning |
|---|---|
| `0 < value <= 1` | Fraction of transactions, e.g. `0.2` means 20% |
| `value > 1` | Absolute support count |

`--min-support` must be greater than `0`. Invalid values fail before Spark starts the mining work.

---

### 3.1 Run DApriori

```bash
spark-submit dapriori.py otto_transactions.txt \
  --min-support 0.2 \
  --delimiter "," \
  --master local[*]
```

Save output:

```bash
spark-submit dapriori.py otto_transactions.txt \
  --min-support 0.2 \
  --delimiter "," \
  --master local[*] \
  --output output_dapriori
```

---

### 3.2 Run DATID

```bash
spark-submit datid.py otto_transactions.txt \
  --min-support 0.2 \
  --delimiter "," \
  --master local[*]
```

Save output:

```bash
spark-submit datid.py otto_transactions.txt \
  --min-support 0.2 \
  --delimiter "," \
  --master local[*] \
  --output output_datid
```

---

### 3.3 Run DECLAT

```bash
spark-submit declat.py otto_transactions.txt \
  --min-support 0.2 \
  --delimiter "," \
  --master local[*]
```

Save output:

```bash
spark-submit declat.py otto_transactions.txt \
  --min-support 0.2 \
  --delimiter "," \
  --master local[*] \
  --output output_declat
```

---

## 4. Run Association Rule Mining

Use `spark_arm.py` to generate association rules from transactions.

Supported algorithms:

- `dapriori`
- `datid`
- `declat`

Example using DATID:

```bash
spark-submit spark_arm.py otto_transactions.txt \
  --algorithm datid \
  --min-support 0.2 \
  --min-confidence 0.7 \
  --delimiter "," \
  --master local[*]
```

Save output:

```bash
spark-submit spark_arm.py otto_transactions.txt \
  --algorithm datid \
  --min-support 0.2 \
  --min-confidence 0.7 \
  --delimiter "," \
  --master local[*] \
  --output output_rules_datid
```

Example using DApriori:

```bash
spark-submit spark_arm.py otto_transactions.txt \
  --algorithm dapriori \
  --min-support 0.2 \
  --min-confidence 0.7 \
  --delimiter "," \
  --master local[*] \
  --output output_rules_dapriori
```

Example using DECLAT:

```bash
spark-submit spark_arm.py otto_transactions.txt \
  --algorithm declat \
  --min-support 0.2 \
  --min-confidence 0.7 \
  --delimiter "," \
  --master local[*] \
  --output output_rules_declat
```

---

## 5. Output Format

Frequent itemset output:

```text
feat_1,feat_5    count=23000    support=0.255556
```

Association rule output:

```text
feat_1,feat_5 -> target_Class_2    confidence=0.750000    support_count=23000    support=0.255556
```

If `--output` is used, Spark creates an output directory containing files like:

```text
part-00000
part-00001
_SUCCESS
```

To view results:

```bash
cat output_rules_datid/part-*
```

On Windows PowerShell:

```powershell
Get-Content output_rules_datid\part-*
```

---

## 6. Important Notes

1. Spark output directories must not already exist. If the output directory exists, delete it first:

```bash
rm -rf output_rules_datid
```

PowerShell:

```powershell
Remove-Item -Recurse -Force output_rules_datid
```

2. Otto has many features. If execution is slow, increase `min-support`:

```bash
--min-support 0.3
```

3. If the output is too small, decrease `min-support` carefully:

```bash
--min-support 0.05
```

4. For complete Association Rule Mining, `datid` is usually a good default choice:

```bash
spark-submit spark_arm.py otto_transactions.txt \
  --algorithm datid \
  --min-support 0.2 \
  --min-confidence 0.7 \
  --delimiter "," \
  --master local[*] \
  --output output_rules_datid
```

5. The frequent itemset implementations intentionally cache and materialize intermediate Spark RDDs before unpersisting parent RDDs. This keeps the lazy execution lineage stable during longer runs, especially in `datid.py` and `declat.py`.

---

## 7. Minimal End-to-End Example

```bash
python prepare_otto_transactions.py train.csv \
  --output otto_transactions.txt \
  --include-target

spark-submit spark_arm.py otto_transactions.txt \
  --algorithm datid \
  --min-support 0.2 \
  --min-confidence 0.7 \
  --delimiter "," \
  --master local[*] \
  --output output_rules_datid

cat output_rules_datid/part-*
```
