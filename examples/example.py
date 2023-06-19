from argparse import ArgumentParser
import csv
import json

from pyspark.sql import SparkSession

from pyspark_diff import diff_df


def process(input_left, input_right, output):
    spark = (
        SparkSession.builder.config("spark.driver.memory", "8g")
        .appName(__name__)
        .getOrCreate()
    )

    left_df = spark.read.json(input_left)
    right_df = spark.read.json(input_right)

    df = diff_df(left_df, right_df, id_field="id")

    changes = []
    for row in df.collect():
        for change in row["changes"]:
            changes.append(
                {
                    "id": row["id"],
                    "diff": row["diff"],
                    "key": change,
                    "left_value": row[f"left_{change}"],
                    "right_value": row[f"right_{change}"],
                    "left": json.dumps(
                        left_df.select("*")
                        .where(left_df.id == row["id"])
                        .collect()[0]
                        .asDict(recursive=True)
                    ),
                    "right": json.dumps(
                        right_df.select("*")
                        .where(right_df.id == row["id"])
                        .collect()[0]
                        .asDict(recursive=True)
                    ),
                }
            )

    if changes:
        with open(output, "w") as fd:
            writer = csv.DictWriter(fd, fieldnames=changes[0].keys())
            writer.writeheader()
            writer.writerows(changes)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-l", "--input-left-json-filepath")
    parser.add_argument("-r", "--input-right-json-filepath")
    parser.add_argument("-o", "--output-differences-csv-filepath")
    args = parser.parse_args()
    process(
        args.input_left_json_filepath,
        args.input_right_json_filepath,
        args.output_differences_csv_filepath,
    )
