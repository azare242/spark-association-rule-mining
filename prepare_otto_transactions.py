import pandas as pd

input_file = "train.csv"
output_file = "otto_transactions.txt"

df = pd.read_csv(input_file)

feature_cols = [c for c in df.columns if c.startswith("feat_")]

with open(output_file, "w", encoding="utf-8") as f:
    for _, row in df.iterrows():
        items = []

        for c in feature_cols:
            if row[c] > 0:
                items.append(c)

        # optional: include target as an item
        if "target" in df.columns:
            items.append("target_" + str(row["target"]))

        f.write(",".join(items) + "\n")