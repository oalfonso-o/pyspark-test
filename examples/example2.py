import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)
from pyspark_diff import diff

logger = logging.getLogger("example2")

spark = SparkSession.builder.appName(__name__).getOrCreate()

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("list", ArrayType(StringType(), True), True),
        StructField(
            "cpg1",
            StructType(
                [
                    StructField("cpg2", StringType(), True),
                    StructField(
                        "cpg3",
                        StructType(
                            [
                                StructField(
                                    "cpg4",
                                    ArrayType(
                                        StructType(
                                            [StructField("cpg5", IntegerType(), True)]
                                        )
                                    ),
                                )
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

NUM_ITEMS = 1_000

data1 = [
    {
        "id": i,
        "list": ["list1", "list2"],
        "cpg1": {"cpg2": "2_value", "cpg3": {"cpg4": [{"cpg5": 1}]}},
    }
    for i in range(NUM_ITEMS)
]
data2 = [
    {
        "id": i,
        "list": ["list1", "list2"],
        "cpg1": {"cpg2": "2_value", "cpg3": {"cpg4": [{"cpg5": 2}, {"cpg6": 2}]}},
    }
    for i in range(NUM_ITEMS)
]

logger.info(f"Datasets with {NUM_ITEMS} items...")

left_df = spark.createDataFrame(data1, schema=schema)
right_df = spark.createDataFrame(data2, schema=schema)

# join is very expensive with 1m items, faster in streaming without spark for now
rdd = diff(left_df, right_df, id_fields=["id"])

logger.info(rdd.take(100))
