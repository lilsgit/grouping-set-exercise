# Framework 3: pyspark
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, max, sum, when, col

spark = SparkSession.builder \
    .appName('datasetExercise') \
    .getOrCreate()

# Read the data from BigQuery
df_main = spark.read.format("bigquery") \
    .option("table", "dt-lilytian-sandbox-dev.pyspark_data.dataset1") \
    .load()
df_main.createOrReplaceTempView("df_main")
df_dict = spark.read.format("bigquery") \
    .option("table", "dt-lilytian-sandbox-dev.pyspark_data.dataset2") \
    .load()
df_dict.createOrReplaceTempView("df_dict")

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

# Saving the data to BigQuery
df5.write.format("bigquery") \
    .option("writeMethod", "direct") \
    .save("pyspark_data.dataset_output")
