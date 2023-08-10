# Framework 3: pyspark
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, max, sum, when, col

spark = SparkSession.builder \
    .appName('datasetExercise') \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Read the data from the csv file
df_main = spark.read.csv('./data/dataset1.csv', header=True, inferSchema=True)
df_dict = spark.read.csv('./data/dataset2.csv', header=True, inferSchema=True)

# Cache the DataFrames for reuse
df_main.cache()
df_dict.cache()

# Merge the two dataframes
df_merged = df_main.join(df_dict.hint("broadcast"), on='counter_party', how='inner')


# Function to calculate the metrics
def agg_group_by_cols(group_by_cols: [str], the_other_cols: [str]):
    count_expressions = [count(col_name).alias(col_name) for col_name in the_other_cols]
    sum_if = lambda pyspark_expr, col: sum(when(pyspark_expr, col).otherwise(0))

    return df_merged \
        .groupBy(group_by_cols) \
        .agg(*count_expressions,
             max('rating').alias("max_rating"),
             sum_if(col("status") == "ARAP", col('value')).alias("sum_value_ARAP"),
             sum_if(col("status") == "ACCR", col('value')).alias("sum_value_ACCR"),
             ) \
        .select('legal_entity', 'counter_party', 'tier', 'max_rating', 'sum_value_ARAP', 'sum_value_ACCR')


# Generate all the group by pairs needed
def generate_all_pairs(*group_by_names):
    all_mappings = []

    for i, col in enumerate(group_by_names):
        all_mappings.append(([col], [name for name in group_by_names if name != col]))
        for j in range(i + 1, len(group_by_names)):
            all_mappings.append(([col, group_by_names[j]],
                                 [name for name in group_by_names if name != col and name != group_by_names[j]]))

    return all_mappings


group_by_cols = ('legal_entity', 'counter_party', 'tier')
all_pairs = generate_all_pairs(*group_by_cols)

# Apply the function to all the pairs and union the results
df5 = reduce(DataFrame.unionAll,
             [agg_group_by_cols(group_by_cols, the_other_cols) for group_by_cols, the_other_cols in all_pairs])
df5.show()
# +------------+-------------+----+----------+--------------+--------------+
# |legal_entity|counter_party|tier|max_rating|sum_value_ARAP|sum_value_ACCR|
# +------------+-------------+----+----------+--------------+--------------+
# |          L1|            6|   6|         6|            85|           100|
# |          L3|            6|   6|         6|           145|           205|
# |          L2|            6|   6|         6|          1020|           207|
# |           3|           C6|   3|         6|           145|            60|
# |           5|           C3|   5|         6|             5|           197|
# |           2|           C4|   2|         6|            40|           100|
# |           3|           C5|   3|         6|          1000|           115|
# |           3|           C1|   3|         3|            40|             0|
# |           2|           C2|   2|         3|            20|            40|
# |           3|            3|   1|         3|            40|             0|
# |           3|            3|   6|         6|           145|            60|
# |           5|            5|   3|         6|             5|           197|
# |           3|            3|   5|         6|          1000|           115|
# |           2|            2|   4|         6|            40|           100|
# |           2|            2|   2|         3|            20|            40|
# |          L1|           C4|   2|         6|            40|           100|
# |          L3|           C6|   3|         6|           145|            60|
# |          L2|           C2|   2|         3|            20|            40|
# |          L2|           C5|   3|         6|          1000|           115|
# |          L3|           C3|   3|         4|             0|           145|
# +------------+-------------+----+----------+--------------+--------------+
# only showing top 20 rows


# extra thoughts:
# grouping object:
# filter first - assign filter to different partition
# basic idea: using different nodes in spark
