# framework 1: pandas

import pandas as pd

# read the data from the csv file
df1 = pd.read_csv('dataset1.csv')
df2 = pd.read_csv('dataset2.csv')

# merge the two dataframes
df3_first_merge = pd.merge(df1, df2, on='counter_party', how='inner')


# function to aggregate the data
def agg_group_by_cols(group_by_cols: [str], the_other_cols: [str]):
    agg_dict = {
        f"{col}": (col, 'count') for col in the_other_cols
    }
    agg_dict.update({
        "max_rating": ('rating', 'max'),
        "sum_of_ARAP": ('value', lambda x: x[df3_first_merge['status'] == 'ARAP'].sum()),
        "sum_of_ACCR": ('value', lambda x: x[df3_first_merge['status'] == 'ACCR'].sum())
    })
    return df3_first_merge.groupby(group_by_cols).agg(**agg_dict).reset_index()[
        ['legal_entity', 'counter_party', 'tier', 'max_rating', 'sum_of_ARAP', 'sum_of_ACCR']]


all_mappings = [
    (['legal_entity'], ['counter_party', 'tier']),
    (['counter_party'], ['legal_entity', 'tier']),
    (['tier'], ['legal_entity', 'counter_party']),
    (['legal_entity', 'counter_party'], ['tier']),
    (['legal_entity', 'tier'], ['counter_party']),
    (['counter_party', 'tier'], ['legal_entity'])
]

all_dataframes = []
for group_by_cols, the_other_cols in all_mappings:
    df = agg_group_by_cols(group_by_cols, the_other_cols)
    all_dataframes.append(df)
df5 = pd.concat(all_dataframes, axis=0, ignore_index=False)
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
