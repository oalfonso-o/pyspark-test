# pyspark_diff

Given two dataframes get the list of the differences in all the nested fields, knowing the position of the array items where a value changes and the key of the structs of the value that is different.

Example data:

`/tmp/data1.json`
``` json
{"id": 1, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
{"id": 2, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
{"id": 3, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
```

`/tmp/data2.json`
``` json
{"id": 1, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
{"id": 2, "values_list": ["a", "bdiff"], "values_dict": {"a": "b"}}
{"id": 3, "values_list": ["a", "b"], "values_dict": {"a": "bdiff"}}
```

## diff
Returns a list of differences between two PySpark dataframes.

Example:

``` python
from pyspark.sql import SparkSession
from pyspark_diff import diff

spark = SparkSession.builder.appName(__name__).getOrCreate()

df1 = spark.read.json("/tmp/data1.json")
df2 = spark.read.json("/tmp/data2.json")

differences = diff(
    left_df=df1,
    right_df=df2,
    id_field="id",
    order_by=["id"],
)
```

And `differences` look like this:
``` python
[
    Difference(
        row_id=2,
        column_name="[1]",
        column_name_parent="values_list",
        left="b",
        right="bdiff",
        reason="diff_value",
    ),
    Difference(
        row_id=3,
        column_name="a",
        column_name_parent="values_dict",
        left="b",
        right="bdiff",
        reason="diff_value",
    ),
]
```

We can see a difference in the row `2`(row_id) in the element with position `1` (column_name) of the field `values_list` (column_name_parent) knowing the values of:

- left value: "b"
- right value: "bdiff"

In the left dataframe we had a `b` and in the right dataframe we have a `bdiff`, knowing exactly the position of the array that changes.

The same happens in the second difference but with an struct.

## diff_wip
Named as *WIP* to make it clear that is still not ready.
The idea is to check the differences using Spark, so making it faster and more efficient but so far all the tests have been:

- slower than the `diff`
- have more bugs (no bugs found in `diff`)

The idea is to eventually make this method the default `diff`.

Returns a new RDD with the differences between two PySpark dataframes. Example:

``` python
from pyspark.sql import SparkSession
from pyspark_diff import diff_wip

spark = SparkSession.builder.appName(__name__).getOrCreate()

df1 = spark.read.json("/tmp/data1.json")
df2 = spark.read.json("/tmp/data2.json")

differences = diff_wip(
    left_df=df1,
    right_df=df2,
    id_fields=["id"],
)
```

If we do a `take` of the RDD we get::
``` python
>>> differences.take(2)
[
    {
        "id": (("id", "2"),),
        "differences": [
            '"values_list.1" has different value. Left: <str>"b" - Right: <str>"bdiff"'
        ],
        "left": {"id": 2, "values_dict": {"a": "b"}, "values_list": ["a", "b"]},
        "right": {"id": 2, "values_dict": {"a": "b"}, "values_list": ["a", "bdiff"]},
    },
    {
        "id": (("id", "3"),),
        "differences": [
            '"values_dict.a" has different value. Left: <str>"b" - Right: <str>"bdiff"'
        ],
        "left": {"id": 3, "values_dict": {"a": "b"}, "values_list": ["a", "b"]},
        "right": {"id": 3, "values_dict": {"a": "bdiff"}, "values_list": ["a", "b"]},
    },
]
```

With this method we try to be more verbose than with `diff` as we are gouping the differences per each id in the `differences` list, but it's still not clear which is the best way to extract the diff.


# Documentation

For parameters documentation for now check directly the method as it's still changing because it's in dev mode and the readme is not always updated:

https://github.com/oalfonso-o/pyspark_diff/blob/main/pyspark_diff/pyspark_diff.py


# Similar projects

In case this code is not solving your problem maybe other tools can do it, they have been reviewed while implementing this code:

- Datacompy: https://github.com/capitalone/datacompy
- G-Research: spark-expension.diff: https://github.com/G-Research/spark-extension/blob/master/DIFF.md

Both tools are great but I couldn't find in them the level of flexibility to detail in depth which is the difference in nested fields.

-----

Note:

Initially forked from https://github.com/debugger24/pyspark-test as this repo was intended to add minor features and open a pull request to the original repo but now the idea of this project is not testing pyspark and more about extracting a diff from the pyspark dataframes nested fields.
