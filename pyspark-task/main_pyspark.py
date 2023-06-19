# Framework 3: pyspark

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum as spark_sum, col

spark = SparkSession.builder.appName('datasetExercise').getOrCreate()

# Read the data from the csv file
df1 = spark.read.csv('dataset1.csv', header=True, inferSchema=True)
df2 = spark.read.csv('dataset2.csv', header=True, inferSchema=True)

# Merge the two dataframes
df3_first_merge = df1.join(df2, on='counter_party', how='inner')

# -----------------------------------------------------------------------------------------------------------------------
# SOLUTION 1
# Generate desired output
# legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP),
# sum(value where status=ACCR)
df3_groupby = df3_first_merge \
    .groupby(['legal_entity', 'counter_party', 'tier', 'status']) \
    .agg({'rating': 'max', 'value': 'sum'}) \
    .withColumnRenamed('max(rating)', 'max_rating') \
    .withColumnRenamed('sum(value)', 'sum_value')

df3_sum_arap = df3_first_merge \
    .where(df3_groupby.status == 'ARAP') \
    .groupby(['legal_entity', 'counter_party', 'tier']) \
    .sum('value') \
    .withColumnRenamed('sum(value)', 'sum_value_arap')
# print(df3_sum_arap.show())

# create new record to add total for each of legal entity, counterparty & tier.
df4_add_total = df3_groupby.withColumn('total_all_status',
                                       spark_sum('sum_value').over(
                                           Window.partitionBy('legal_entity', 'counter_party', 'tier')))

# print(df4_add_total.show())
# +------------+-------------+----+------+----------+---------+----------------+
# |legal_entity|counter_party|tier|status|max_rating|sum_value|total_all_status|
# +------------+-------------+----+------+----------+---------+----------------+
# |          L1|           C1|   1|  ARAP|         3|       40|              40|
# |          L1|           C3|   3|  ARAP|         6|        5|               5|
# |          L1|           C4|   4|  ARAP|         6|       40|             140|
# |          L1|           C4|   4|  ACCR|         5|      100|             140|
# |          L2|           C2|   2|  ACCR|         3|       40|              60|
# |          L2|           C2|   2|  ARAP|         2|       20|              60|
# |          L2|           C3|   3|  ACCR|         2|       52|              52|
# |          L2|           C5|   5|  ARAP|         6|     1000|            1115|
# |          L2|           C5|   5|  ACCR|         4|      115|            1115|
# |          L3|           C3|   3|  ACCR|         4|      145|             145|
# |          L3|           C6|   6|  ACCR|         6|       60|             205|
# |          L3|           C6|   6|  ARAP|         5|      145|             205|
# +------------+-------------+----+------+----------+---------+----------------+


# -----------------------------------------------------------------------------------------------------------------------
# BETTER SOLUTIONS
# Try out the pivot and unpivot functions in pyspark
df3_max = df3_first_merge.groupby(['legal_entity', 'counter_party', 'tier']).max(
    "rating").withColumnRenamed(
    'max(rating)', 'max_rating')
df3_sum = df3_first_merge.groupby(['legal_entity', 'counter_party', 'tier']).pivot('status').sum(
    'value').withColumnRenamed('ACCR', 'sum_value_accr').withColumnRenamed('ARAP', 'sum_value_arap')

df4 = df3_max.join(df3_sum, on=['legal_entity', 'counter_party', 'tier'])

# create new record to add total for each of legal entity, counterparty & tier.
df5 = df4.na.fill(0).withColumn('total_all_status', col('sum_value_accr') + col('sum_value_arap'))
df5.show()
# +------------+-------------+----+----------+--------------+--------------+----------------+
# |legal_entity|counter_party|tier|max_rating|sum_value_accr|sum_value_arap|total_all_status|
# +------------+-------------+----+----------+--------------+--------------+----------------+
# |          L2|           C2|   2|         3|            40|            20|              60|
# |          L1|           C1|   1|         3|             0|            40|              40|
# |          L1|           C3|   3|         6|             0|             5|               5|
# |          L2|           C3|   3|         2|            52|             0|              52|
# |          L3|           C3|   3|         4|           145|             0|             145|
# |          L2|           C5|   5|         6|           115|          1000|            1115|
# |          L1|           C4|   4|         6|           100|            40|             140|
# |          L3|           C6|   6|         6|            60|           145|             205|
# +------------+-------------+----+----------+--------------+--------------+----------------+
