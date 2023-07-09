import json
import logging

import flatdict
from pyspark.sql import SparkSession

logger = logging.getLogger("example3")

spark = SparkSession.builder.appName(__name__).getOrCreate()
sc = spark.sparkContext

data = [
    {
        "id": i,
        "list": ["list1", "list2"],
        "cpg1": {"cpg2": "2_value", "cpg3": {"cpg4": [{"cpg5": 1}]}},
    }
    for i in range(10)
]

logger.info(
    "Tests with writing and reading jsons to DF and RDDs and flattening the data, no diffs here"
)

file_data = "\n".join([json.dumps(r) for r in data])
tmpfile = "tmpdata.json"

with open(tmpfile, "w") as fd:
    fd.write(file_data)

df = spark.read.json(tmpfile)  # better schema infering than spark.createDataFrame
rdd = df.rdd
rdd = rdd.map(lambda row: dict(flatdict.FlatterDict(row.asDict(recursive=True), ".")))
df = rdd.toDF()
df = df.orderBy(["id"])

df.show()
