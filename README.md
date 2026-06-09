# Spark Association Rule Mining

This project contains PySpark implementations for frequent itemset mining and association rule mining.

Implemented files:

| File | Purpose |
|---|---|
| `dapriori.py` | Runs Distributed Apriori and outputs frequent itemsets |
| `datid.py` | Runs Distributed Apriori-TID and outputs frequent itemsets |
| `declat.py` | Runs Distributed ECLAT and outputs frequent itemsets |
| `spark_arm.py` | Runs association rule mining using one of the frequent itemset algorithms |
| `convert_transactions_to_parquet.py` | Converts text transactions into Parquet for streaming |
| `spark_arm_stream.py` | Runs frequent itemset mining or association rule mining on Spark stream micro-batches |

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

## 3. Convert Transactions to Parquet for Streaming

`spark_arm_stream.py` can read text streams or Parquet streams. For the Otto transaction file, convert `otto_transactions.txt` into Parquet first:

```bash
spark-submit convert_transactions_to_parquet.py otto_transactions.txt \
  --output otto_transactions_parquet \
  --delimiter "," \
  --overwrite \
  --master local[*]
```

The Parquet output schema is:

| Column | Type | Meaning |
|---|---|---|
| `transaction_id` | `long` | Generated Spark row id |
| `raw_line` | `string` | Original transaction text |
| `items` | `array<string>` | Parsed transaction items |
| `item_count` | `long` | Number of items in the transaction |

Use `--overwrite` only when you want to replace an existing Parquet directory. Without it, Spark fails if the output already exists.

---

## 4. Run Frequent Itemset Mining

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

### 4.1 Run DApriori

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

### 4.2 Run DATID

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

### 4.3 Run DECLAT

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

## 5. Run Association Rule Mining

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

## 6. Run Streaming ARM

`spark_arm_stream.py` uses Spark Structured Streaming with `foreachBatch`. Each micro-batch is converted into the same transaction RDD format used by `dapriori.py`, `datid.py`, and `declat.py`.

By default, the stream uses `availableNow=True`: Spark processes files currently available in the input directory and exits. Add `--continuous` to keep the stream running for new files.

### 6.1 Streaming Frequent Itemsets from Parquet

```bash
spark-submit spark_arm_stream.py otto_transactions_parquet \
  --source-format parquet \
  --mode frequent-itemsets \
  --algorithm dapriori \
  --min-support 0.45 \
  --checkpoint checkpoints/stream_fim_dapriori \
  --master local[*]
```

Save output per micro-batch:

```bash
spark-submit spark_arm_stream.py otto_transactions_parquet \
  --source-format parquet \
  --mode frequent-itemsets \
  --algorithm datid \
  --min-support 0.45 \
  --checkpoint checkpoints/stream_fim_datid \
  --output stream_fim_datid_out \
  --master local[*]
```

Output is written under batch directories:

```text
stream_fim_datid_out/batch_id=0/part-00000
```

### 6.2 Streaming Association Rules from Parquet

```bash
spark-submit spark_arm_stream.py otto_transactions_parquet \
  --source-format parquet \
  --mode rules \
  --algorithm declat \
  --min-support 0.45 \
  --min-confidence 0.75 \
  --checkpoint checkpoints/stream_rules_declat \
  --output stream_rules_declat_out \
  --master local[*]
```

### 6.3 Streaming from Text

For text streaming, pass an input directory containing transaction text files. Spark file streams are designed around directories of files rather than a single file that is repeatedly modified.

```bash
spark-submit spark_arm_stream.py stream_text_input \
  --source-format text \
  --mode frequent-itemsets \
  --algorithm dapriori \
  --min-support 0.45 \
  --delimiter "," \
  --checkpoint checkpoints/stream_text_dapriori \
  --master local[*]
```

### 6.4 Streaming Algorithm Choices

`--algorithm` accepts:

- `dapriori`
- `datid`
- `declat`

`--min-support` is evaluated per micro-batch. A value in `(0, 1]` is a fraction of the transactions in that micro-batch; a value greater than `1` is an absolute support count.

---

## 7. Output Format

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

## 8. Important Notes

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

6. Streaming output directories are created per micro-batch, for example `stream_rules_declat_out/batch_id=0`. The same Spark rule applies: output and checkpoint directories should not conflict with old runs unless you intentionally reuse checkpoints.

---

## 9. Minimal End-to-End Example

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

Streaming Parquet path:

```bash
python prepare_otto_transactions.py train.csv \
  --output otto_transactions.txt \
  --include-target

spark-submit convert_transactions_to_parquet.py otto_transactions.txt \
  --output otto_transactions_parquet \
  --delimiter "," \
  --overwrite \
  --master local[*]

spark-submit spark_arm_stream.py otto_transactions_parquet \
  --source-format parquet \
  --mode rules \
  --algorithm datid \
  --min-support 0.45 \
  --min-confidence 0.75 \
  --checkpoint checkpoints/stream_rules_datid \
  --output stream_rules_datid_out \
  --master local[*]

cat stream_rules_datid_out/batch_id=0/part-*
```
