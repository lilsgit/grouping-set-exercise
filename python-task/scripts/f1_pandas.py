# framework 1: pandas

import pandas as pd

# read the data from the csv file
df_main = pd.read_csv('./data/dataset1.csv')
df_dict = pd.read_csv('./data/dataset2.csv')

# merge the two dataframes
df_merged = pd.merge(df_main, df_dict, on='counter_party', how='inner')


# function to calculate the metrics
def calculate_metrics(dataframe, max_col, sum_col, filter_col, filter_condition1, filter_condition2):
    max_v = (max_col, 'max')
    sum_value_cond1 = (sum_col, lambda x: x[dataframe[filter_col] == filter_condition1].sum())
    sum_value_cond2 = (sum_col, lambda x: x[dataframe[filter_col] == filter_condition2].sum())

    return {
        f"max_{max_col}": max_v,
        f"sum_{sum_col}_{filter_condition1}": sum_value_cond1,
        f"sum_{sum_col}_{filter_condition2}": sum_value_cond2
    }


# all the desired columns
desired_columns = ['legal_entity', 'counter_party', 'tier', 'max_rating', 'sum_value_ARAP', 'sum_value_ACCR']


# function to aggregate the data
def agg_group_by_cols(group_by_cols: [str], the_other_cols: [str]):
    agg_dict = {
        f"{col}": (col, 'count') for col in the_other_cols
    }
    agg_dict.update(calculate_metrics(df_merged, 'rating', 'value', 'status', 'ARAP', 'ACCR'))

    return df_merged.groupby(group_by_cols).agg(**agg_dict).reset_index()[desired_columns]


# generate all the group by pairs needed
def generate_all_pairs(*group_by_names):
    all_mappings = []

    for i, col in enumerate(group_by_names):
        all_mappings.append(([col], [name for name in group_by_names if name != col]))
        for j in range(i + 1, len(group_by_names)):
            all_mappings.append(([col, group_by_names[j]],
                                 [name for name in group_by_names if name != col and name != group_by_names[j]]))

    return all_mappings


col_names_for_grouping = ('legal_entity', 'counter_party', 'tier')
all_pairs = generate_all_pairs(*col_names_for_grouping)


def union_all_dataframes(all_mappings_list: [([str], [str])]):
    all_dataframes = []
    for group_by_cols, the_other_cols in all_mappings_list:
        df_final = agg_group_by_cols(group_by_cols, the_other_cols)
        all_dataframes.append(df_final)
    return pd.concat(all_dataframes, axis=0, ignore_index=False)


df5 = union_all_dataframes(all_pairs)
print(df5)
#   legal_entity counter_party  tier  max_rating  sum_of_ARAP  sum_of_ACCR
# 0           L1             6     6           6           85          100
# 1           L2             6     6           6         1020          207
# 2           L3             6     6           6          145          205
# 0            3            C1     3           3           40            0
# 1            2            C2     2           3           20           40
# 2            5            C3     5           6            5          197
# 3            2            C4     2           6           40          100
# 4            3            C5     3           6         1000          115
# 5            3            C6     3           6          145           60
# 0            3             3     1           3           40            0
# 1            2             2     2           3           20           40
# 2            5             5     3           6            5          197
# 3            2             2     4           6           40          100
# 4            3             3     5           6         1000          115
# 5            3             3     6           6          145           60
# 0           L1            C1     3           3           40            0
# 1           L1            C3     1           6            5            0
# 2           L1            C4     2           6           40          100
# 3           L2            C2     2           3           20           40
# 4           L2            C3     1           2            0           52
# 5           L2            C5     3           6         1000          115
# 6           L3            C3     3           4            0          145
# 7           L3            C6     3           6          145           60
# 0           L1             3     1           3           40            0
# 1           L1             1     3           6            5            0
# 2           L1             2     4           6           40          100
# 3           L2             2     2           3           20           40
# 4           L2             1     3           2            0           52
# 5           L2             3     5           6         1000          115
# 6           L3             3     3           4            0          145
# 7           L3             3     6           6          145           60
# 0            3            C1     1           3           40            0
# 1            2            C2     2           3           20           40
# 2            5            C3     3           6            5          197
# 3            2            C4     4           6           40          100
# 4            3            C5     5           6         1000          115
# 5            3            C6     6           6          145           60
