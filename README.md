# pyspark_diff

You have two ways to get the differences, you can get a list of Python pyspark_diff.Difference objects or getting a new dataframe with one row per each difference.

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

## diff_df
Returns a new dataframe with the differences between two PySpark dataframes. Example:

``` python
from pyspark.sql import SparkSession
from pyspark_diff import diff_df

spark = SparkSession.builder.appName(__name__).getOrCreate()

df1 = spark.read.json("/tmp/data1.json")
df2 = spark.read.json("/tmp/data2.json")

differences = diff_df(
    left_df=df1,
    right_df=df2,
    id_field="id",
)
```

If we do a show of the dataframe we see this:
```
>>> differences.show()
+----+----------------+---+-------------------+--------------------+-------------------+--------------------+-------------------+--------------------+
|diff|         changes| id|left_values_list[0]|right_values_list[0]|left_values_list[1]|right_values_list[1]|left_values_dict__a|right_values_dict__a|
+----+----------------+---+-------------------+--------------------+-------------------+--------------------+-------------------+--------------------+
|   C|[values_list[1]]|  2|                  a|                   a|                  b|               bdiff|                  b|                   b|
|   C|[values_dict__a]|  3|                  a|                   a|                  b|                   b|                  b|               bdiff|
+----+----------------+---+-------------------+--------------------+-------------------+--------------------+-------------------+--------------------+
```

The dataframe will contain the diff column informing which type of difference is, the list of fields which changed for that row, the id, and then all the input columns flattened with it's corresponding `left_*` and `right_*` prefix to be able to compare.

The nested fields are flattened following this system:
- arrays -> will have the name of the field + [array_index]. For example: `artists[0]`
- structs -> will have the name of the field + double underscore + the name of the key. For example: `artist__name`

Then we can use this data as we wish, in the `examples/example.py` we are adding the original row and formatting the column names for a custom csv output. Example:
```
python examples/example.py -l /tmp/data1.json -r /tmp/data2.json -o differences.csv
```
Running this example.py we will get the dataframe, parse the output and write a new csv with the differences which will look like this:
|id |diff|key           |left_value|right_value|left                                                           |right                                                              |
|---|----|--------------|----------|-----------|---------------------------------------------------------------|-------------------------------------------------------------------|
|2  |C   |values_list[1]|b         |bdiff      |{"id": 2, "values_dict": {"a": "b"}, "values_list": ["a", "b"]}|{"id": 2, "values_dict": {"a": "b"}, "values_list": ["a", "bdiff"]}|
|3  |C   |values_dict__a|b         |bdiff      |{"id": 3, "values_dict": {"a": "b"}, "values_list": ["a", "b"]}|{"id": 3, "values_dict": {"a": "bdiff"}, "values_list": ["a", "b"]}|

The output informs the ID of the row that contains a difference, also which is the key (useful for highly nested data models), value in each field and also the original row for debugging.
## diff_objs
Returns a list of differences between two PySpark dataframes.

It's better to use the `diff_df` as it processes everything with spark dataframes and it's more optimal. This method first performs a `collect` on both dataframes and does all the comparisons using plain python after loading everything into memory. This method can be interesting for custom processing but is not suitable with large datasets if we don't have enough memory available. Also it can be slower.

Example:

``` python
from pyspark.sql import SparkSession
from pyspark_diff import diff_objs

spark = SparkSession.builder.appName(__name__).getOrCreate()

df1 = spark.read.json("/tmp/data1.json")
df2 = spark.read.json("/tmp/data2.json")

differences = diff_objs(
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

The output is not exactly the same and this method won't be continued as it's preferred to use the `diff_df` method.

# Documentation

For parameters documentation for now check directly the method as it's still changing because it's in dev mode and the readme is not always updated:

https://github.com/oalfonso-o/pyspark_diff/blob/main/pyspark_diff/pyspark_diff.py


-----

Note:

Initially forked from https://github.com/debugger24/pyspark-test as this repo was intended to add minor features and open a pull request to the original repo but now the idea of this project is not testing pyspark and more about extracting a diff from the pyspark dataframes. So the purpose changed from testing to debugging.
