import logging
import uuid

from gresearch.spark.diff import DiffOptions, diff_with_options
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql import DataFrame

from pyspark_diff.models import Difference


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s: %(message)s", level="INFO"
)
logger = logging.getLogger("pyspark_test")

REASON_DIFF_INPUT_TYPE = "diff_input_type"
REASON_DIFF_COLUMNS = "diff_columns"
REASON_DIFF_SCHEMA = "diff_schema"
REASON_DIFF_ROW_COUNT = "diff_row_count"
REASON_DIFF_TYPE = "diff_type"
REASON_DIFF_VALUE = "diff_value"
REASON_DIFF_LIST_LEN = "diff_list_len"


class SimpleValidator:
    @staticmethod
    def validate_input(
        left_df: pyspark.sql.DataFrame,
        right_df: pyspark.sql.DataFrame,
        id_field: str = None,
        recursive: bool = False,
        columns: list = None,
        return_all_differences: bool = False,
        skip_n_first_rows: int = 0,
        order_by: list = None,
        sorting_keys: dict = None,
    ) -> None:
        if not isinstance(left_df, pyspark.sql.DataFrame) or not isinstance(
            right_df, pyspark.sql.DataFrame
        ):
            raise ValueError(
                "Both inputs must be instances of pyspark.sql.DataFrame, "
                f"found left: {type(left_df)}, right: {type(right_df)}"
            )
        if order_by and not isinstance(order_by, list):
            raise ValueError(f"order_by must be list, found {type(order_by)}")
        if not isinstance(return_all_differences, bool):
            raise ValueError(
                f"return_all_differences must be bool, found {type(return_all_differences)}"
            )
        if id_field and not isinstance(id_field, str):
            raise ValueError(f"id_field must be str, found {type(id_field)}")
        if not isinstance(recursive, bool):
            raise ValueError(f"recursive must be bool, found {type(recursive)}")
        if skip_n_first_rows and not isinstance(skip_n_first_rows, int):
            raise ValueError(
                f"skip_n_first_rows must be int, found {type(skip_n_first_rows)}"
            )
        if columns and not isinstance(columns, list):
            raise ValueError(f"columns must be list, found {type(columns)}")
        if sorting_keys:
            if not isinstance(sorting_keys, dict):
                raise ValueError(
                    f"sorting_keys must be dict, found {type(sorting_keys)}"
                )
            else:
                for k, v in sorting_keys.items():
                    if not callable(v):
                        raise ValueError(
                            "sorting_keys must be dict and the values must be callables, found "
                            f"{k}:{v}"
                        )

    @staticmethod
    def diff_columns(
        left_df: pyspark.sql.DataFrame,
        right_df: pyspark.sql.DataFrame,
    ) -> list[Difference]:
        differences = []
        left_columns = set(left_df.columns)
        right_columns = set(right_df.columns)
        columns_only_left = left_columns - right_columns
        columns_only_right = right_columns - left_columns

        if columns_only_left or columns_only_right:
            differences.append(
                Difference(
                    row_id=0,
                    column_name="",
                    column_name_parent="",
                    left=columns_only_left,
                    right=columns_only_right,
                    reason=REASON_DIFF_COLUMNS,
                )
            )

        return differences

    @staticmethod
    def diff_schema(
        left_df: pyspark.sql.DataFrame,
        right_df: pyspark.sql.DataFrame,
    ) -> list[Difference]:
        differences = []
        left_dtypes = set(left_df.dtypes)
        right_dtypes = set(right_df.dtypes)
        dtypes_only_left = left_dtypes - right_dtypes
        dtypes_only_right = right_dtypes - left_dtypes

        if dtypes_only_left or dtypes_only_right:
            differences.append(
                Difference(
                    row_id=0,
                    column_name="",
                    column_name_parent="",
                    left=dtypes_only_left,
                    right=dtypes_only_right,
                    reason=REASON_DIFF_SCHEMA,
                )
            )

        return differences

    @staticmethod
    def diff_row_count(left_df, right_df) -> list[Difference]:
        differences = []
        left_df_count = left_df.count()
        right_df_count = right_df.count()
        if left_df_count != right_df_count:
            differences.append(
                Difference(
                    row_id=0,
                    column_name="",
                    column_name_parent="",
                    left=left_df_count,
                    right=right_df_count,
                    reason=REASON_DIFF_ROW_COUNT,
                )
            )

        return differences


class WithoutSpark:
    @classmethod
    def diff_df_content(
        cls,
        left_df: pyspark.sql.DataFrame,
        right_df: pyspark.sql.DataFrame,
        return_all_differences: bool = False,
        id_field: str = None,
        recursive: bool = False,
        skip_n_first_rows: int = 0,
        order_by: list = None,
        columns: list = None,
        sorting_keys: dict = None,
    ) -> list[Difference]:
        differences = []

        left_df_list = left_df.collect()[skip_n_first_rows:]
        right_df_list = right_df.collect()[skip_n_first_rows:]

        if skip_n_first_rows and order_by:
            left_df_list = sorted(left_df_list, key=lambda r: [r[s] for s in order_by])
            right_df_list = sorted(
                right_df_list, key=lambda r: [r[s] for s in order_by]
            )

        if id_field and (
            id_field not in left_df.columns or id_field not in left_df.columns
        ):
            raise ValueError(f"id_field {id_field} not present in the input dataframes")

        for row_index in range(len(left_df_list)):
            row_id = None
            columns = columns or left_df.columns

            if id_field:
                row_id = left_df_list[row_index][id_field]
                if id_field not in columns:
                    columns.append(id_field)

            for column_name in columns:
                left_row = left_df_list[row_index][column_name]
                right_row = right_df_list[row_index][column_name]
                diff = cls._diff_row(
                    left_row=left_row,
                    right_row=right_row,
                    column_name=column_name,
                    row_id=row_id,
                    recursive=recursive,
                    sorting_keys=sorting_keys,
                )
                if diff:
                    differences.extend(diff)
                    if not return_all_differences:
                        return differences

            if row_index % 1_000 == 0:
                logger.info(f"Done {row_index}/{len(left_df_list)}")

        return differences

    @classmethod
    def _diff_row(
        cls,
        left_row,
        right_row,
        column_name,
        row_id,
        recursive,
        column_name_parent: str = "",
        sorting_keys: dict = None,
    ) -> list[Difference]:
        differences = []
        if isinstance(left_row, pyspark.sql.types.Row):
            left_row = left_row.asDict(True)
        if isinstance(right_row, pyspark.sql.types.Row):
            right_row = right_row.asDict(True)
        if left_row and right_row and left_row != right_row:
            diff = Difference(
                row_id=row_id,
                column_name=column_name,
                column_name_parent=column_name_parent,
                left=left_row,
                right=right_row,
            )
            # If not same instance -> no need for more checks, we can't compare
            if not isinstance(left_row, type(right_row)):
                diff.reason = REASON_DIFF_TYPE
                differences.append(diff)
            # Iterate dict recursively if requested
            elif recursive and isinstance(left_row, dict):
                for key in left_row:
                    differences.extend(
                        cls._diff_row(
                            left_row=left_row[key],
                            right_row=right_row[key],
                            column_name=key,
                            row_id=row_id,
                            recursive=recursive,
                            column_name_parent=".".join(
                                filter(bool, [column_name_parent, column_name])
                            ),
                            sorting_keys=sorting_keys,
                        )
                    )
            # Iterate list recursively if requested
            elif recursive and isinstance(left_row, list):
                if len(left_row) != len(right_row):
                    diff.reason = REASON_DIFF_LIST_LEN
                    differences.append(diff)
                else:
                    if sorting_keys and column_name in sorting_keys:
                        left_row = sorted(left_row, key=sorting_keys[column_name])
                        right_row = sorted(right_row, key=sorting_keys[column_name])
                    for i in range(len(left_row)):
                        differences.extend(
                            cls._diff_row(
                                left_row=left_row[i],
                                right_row=right_row[i],
                                column_name=f"[{i}]",
                                row_id=row_id,
                                recursive=recursive,
                                column_name_parent=".".join(
                                    filter(bool, [column_name_parent, column_name])
                                ),
                                sorting_keys=sorting_keys,
                            )
                        )
            else:
                diff.reason = REASON_DIFF_VALUE
                differences.append(diff)

        return differences


class WithSpark:
    NESTED_FIELDS_SEP = "__"

    @classmethod
    def diff_df_content(
        cls,
        left_df: pyspark.sql.DataFrame,
        right_df: pyspark.sql.DataFrame,
        id_field: str,
        order_by: list = None,
        columns: list = None,
        sorting_keys: dict = None,
    ) -> DataFrame:
        if id_field not in left_df.columns or id_field not in left_df.columns:
            raise ValueError(f"id_field {id_field} not present in the input dataframes")

        # avoid ambiguous id cases with a tmp unique id col name
        unique_id_field = uuid.uuid4().hex
        left_df = left_df.withColumnRenamed(id_field, unique_id_field)
        right_df = right_df.withColumnRenamed(id_field, unique_id_field)

        # 1. Flatten
        logger.info("Flattening dataframes...")
        flat_left_df, flat_right_df = cls._flat_dfs(
            left_df, right_df, id_field=unique_id_field
        )

        flat_left_df = flat_left_df.withColumnRenamed(unique_id_field, id_field)
        flat_right_df = flat_right_df.withColumnRenamed(unique_id_field, id_field)

        # 2. Compare
        logger.info("Comparing dataframes...")
        options = DiffOptions().with_change_column("changes")
        diff_df = diff_with_options(
            flat_left_df, flat_right_df, options, id_field
        ).filter(F.col("diff") != DiffOptions.nochange_diff_value)

        return diff_df

    @classmethod
    def _flat_dfs(cls, left_df, right_df, id_field):
        flattened = False
        fields = set(left_df.schema.fields + right_df.schema.fields)
        for field in fields:
            if field.dataType.typeName() == StructType.typeName():
                left_cols = cls._field_columns(left_df, field)
                right_cols = cls._field_columns(right_df, field)

                left_df = left_df.select("*", *left_cols.values()).drop(field.name)
                right_df = right_df.select("*", *right_cols.values()).drop(field.name)

                # add missing cols to keep schema symmetry
                left_colnames = set(left_cols.keys())
                right_colnames = set(right_cols.keys())
                if left_colnames != right_colnames:
                    if only_left_columns := left_colnames - right_colnames:
                        right_df = right_df.withColumns(
                            {
                                # c: F.lit(None).cast(left_df.schema[c])
                                c: F.lit(None)
                                for c in only_left_columns
                            }
                        )
                    if only_right_columns := right_colnames - left_colnames:
                        left_df = left_df.withColumns(
                            {
                                # c: F.lit(None).cast(right_df.schema[c])
                                c: F.lit(None)
                                for c in only_right_columns
                            }
                        )

                flattened = True

            elif field.dataType.typeName() == ArrayType.typeName():
                mx_left_len = (
                    left_df.select(F.max(F.size(field.name)).alias("max"))
                    .collect()[0]
                    .max
                )
                mx_right_len = (
                    right_df.select(F.max(F.size(field.name)).alias("max"))
                    .collect()[0]
                    .max
                )
                max_len = max(mx_left_len, mx_right_len)
                left_df = left_df.select(
                    "*",
                    *[F.col(field.name)[i] for i in range(max_len)],
                ).drop(field.name)
                right_df = right_df.select(
                    "*",
                    *[F.col(field.name)[i] for i in range(max_len)],
                ).drop(field.name)
                flattened = True
        if flattened:
            left_df, right_df = cls._flat_dfs(left_df, right_df, id_field=id_field)
        return left_df, right_df

    @classmethod
    def _field_columns(cls, df, field):
        """Returns a dict where each key is the column name and the value is the pyspark column"""
        cols = {}
        if field.name in df.columns:
            for column in df.select(F.col(f"{field.name}.*")).columns:
                cols[column] = F.col(f"{field.name}.{column}").alias(
                    f"{field.name}{cls.NESTED_FIELDS_SEP}{column}"
                )
        return cols


def diff_objs(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    id_field: str = None,
    recursive: bool = True,
    columns: list = None,
    return_all_differences: bool = True,
    skip_n_first_rows: int = 0,
    order_by: list = None,
    sorting_keys: dict = None,
) -> list[Difference]:
    """
    Used to test if two dataframes are same or not

    Args:
        left_df (pyspark.sql.DataFrame): Left Dataframe
        right_df (pyspark.sql.DataFrame): Right Dataframe
        id_field (str, optional): Name of the column that identifies the same row in both
            dataframes. Used to identify the rows with differences. Defaults to None.
        recursive (bool, optional): Checks for differences will be done until the
            field does not contain another field inside, for example a string. Defaults to True.
        columns (list, optional): Compare only these columns. Defaults to None.
        return_all_differences (bool, optional): Check all the differences in the whole file.
            If False only the first difference will be returned. Defaults to True.
        skip_n_first_rows (int, optional): If provided, the first n rows will be ignored.
            Defaults to 0.
        order_by (list, optional): Order the dataframes by these column names before comparing.
            Defaults to None.
        sorting_keys (dict, optional): Sort the values of specific columns if they are lists based
            on the key provided. The value must be a lambda used in the python `sorted` method.
            Notice that this is not going to change the order of the dataset, only the order of the
            values of an specific column.
            Defaults to None.

    Returns:
        A list of the differences: objects of type pyspark_diff.Difference
    """

    SimpleValidator.validate_input(
        left_df=left_df,
        right_df=right_df,
        id_field=id_field,
        recursive=recursive,
        columns=columns,
        return_all_differences=return_all_differences,
        skip_n_first_rows=skip_n_first_rows,
        order_by=order_by,
        sorting_keys=sorting_keys,
    )

    differences = SimpleValidator.diff_columns(left_df, right_df)
    if differences:
        return differences  # if we have different columns there's no need to check more

    differences = SimpleValidator.diff_schema(left_df, right_df)
    if differences:
        return differences  # if we have different schema there's no need to check more

    differences = SimpleValidator.diff_row_count(left_df, right_df)
    if differences:  # if we have different row count there's no need to check more
        return differences

    if order_by and not skip_n_first_rows:
        # order with pyspark only if we don't need to skip inital rows, otherwise sort with python
        left_df = left_df.orderBy(order_by)
        right_df = right_df.orderBy(order_by)

    differences = WithoutSpark.diff_df_content(
        left_df=left_df,
        right_df=right_df,
        return_all_differences=return_all_differences,
        id_field=id_field,
        recursive=recursive,
        skip_n_first_rows=skip_n_first_rows,
        order_by=order_by,
        columns=columns,
        sorting_keys=sorting_keys,
    )

    return differences


def diff_df(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    id_field: str = None,
    columns: list = None,
    order_by: list = None,
    sorting_keys: dict = None,
) -> DataFrame:
    """
    Used to test if two dataframes are same or not, returns a Dataframe with the differences

    Args:
        left_df (pyspark.sql.DataFrame): Left Dataframe
        right_df (pyspark.sql.DataFrame): Right Dataframe
        id_field (str, optional): Name of the column that identifies the same row in both
            dataframes. Used to identify the rows with differences. Defaults to None.
        columns (list, optional): Compare only these columns. Defaults to None.
        order_by (list, optional): Order the dataframes by these column names before comparing.
            Defaults to None.
        sorting_keys (dict, optional): Sort the values of specific columns if they are lists based
            on the key provided.
            Defaults to None.

    Returns:
        a pyspark.sql.DataFrame
    """

    diff_df = WithSpark.diff_df_content(
        left_df=left_df,
        right_df=right_df,
        id_field=id_field,
        order_by=order_by,
        columns=columns,
        sorting_keys=sorting_keys,
    )

    return diff_df
