from argparse import ArgumentParser
import csv

from pyspark.sql import SparkSession

from pyspark_diff import diff_objs


def process(input_left, input_right, output):
    spark = SparkSession.builder.appName(__name__).getOrCreate()

    left_df = spark.read.json(input_left)
    right_df = spark.read.json(input_right)

    differences = diff_objs(
        left_df,
        right_df,
        id_field="id",
        order_by=["id"],
        ignore_columns=["pronto_id"],
        # sorting_keys={"repertoires": lambda x: x["name"]},
    )

    if differences:
        differences = [dict(d) for d in differences]
        with open(output, "w") as fd:
            writer = csv.DictWriter(fd, fieldnames=differences[0].keys())
            writer.writeheader()
            writer.writerows(differences)


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
