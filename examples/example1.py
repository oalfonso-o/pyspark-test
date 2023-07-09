from argparse import ArgumentParser
import csv
import json
import logging

from pyspark.sql import SparkSession

from pyspark_diff import diff

logger = logging.getLogger("example1")


def process(input_left, input_right, output, ignore_columns):
    spark = (
        SparkSession.builder.config("spark.driver.memory", "10g")
        .appName(__name__)
        .getOrCreate()
    )

    left_df = spark.createDataFrame(
        [
            {"id": 1, "title": "song1", "a": "b"},
            {"id": 2, "title": "song2", "a": "b"},
            {"id": 3, "title": "song3", "a": "b"},
        ]
    )
    right_df = spark.createDataFrame(
        [
            {"id": 1, "title": "song1", "a": 123},
            {"id": 2, "title": "song2", "a": "b"},
            {"id": 3, "title": "song3", "a": "b"},
        ]
    )

    rdd = diff(
        left_df, right_df, id_fields=["id", "title"], ignore_columns=ignore_columns
    )

    changes = []
    for row in rdd.collect():
        first = True
        for difference in row["differences"]:
            if first:
                changes.append(
                    {
                        "id": row["id"],
                        "diff": difference,
                        "left": json.dumps(row["left"], ensure_ascii=False),
                        "right": json.dumps(row["right"], ensure_ascii=False),
                    }
                )
                first = False
            else:
                changes.append(
                    {
                        "id": "",
                        "diff": difference,
                        "left": "",
                        "right": "",
                    }
                )

    if changes:
        if output:
            with open(output, "w") as fd:
                writer = csv.DictWriter(fd, fieldnames=changes[0].keys())
                writer.writeheader()
                writer.writerows(changes)
            logger.info(f"Output stored in {output}")
        else:
            logger.info(f"Changes: {changes}")


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-l", "--input-left-json-filepath")
    parser.add_argument("-r", "--input-right-json-filepath")
    parser.add_argument("-o", "--output-differences-csv-filepath")
    parser.add_argument("-i", "--ignore-columns", nargs="+")
    args = parser.parse_args()
    process(
        args.input_left_json_filepath,
        args.input_right_json_filepath,
        args.output_differences_csv_filepath,
        args.ignore_columns,
    )
