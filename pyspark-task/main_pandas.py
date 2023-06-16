# Framework 1: pandas
import string

import pandas as pd

# Read the data from the csv file
df1 = pd.read_csv('dataset1.csv')
df2 = pd.read_csv('dataset2.csv')

# Merge the two dataframes
df3_first_merge = pd.merge(df1, df2, on='counter_party', how='inner')


# Generate desired output
# legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP),
# sum(value where status=ACCR)
def aggregate_data(this_df: pd.DataFrame, status: string):
    new_df = this_df.groupby(['legal_entity', 'counter_party', 'tier']).apply(lambda x: pd.Series({
        'max_rating': x.loc[x['status'] == status, 'rating'].max(),
        'sum_value': x.loc[x['status'] == status, 'value'].sum()
    })).assign(status=status)

    return new_df


df4_arap = aggregate_data(df3_first_merge, 'ARAP')
df4_accr = aggregate_data(df3_first_merge, 'ACCR')

df4_join = pd.concat([df4_arap, df4_accr], axis=0, ignore_index=False)

# create new record to add total for each of legal entity, counterparty & tier.
df4_join['total_all_status'] = df4_join['sum_value'] \
    .groupby(['legal_entity', 'counter_party', 'tier']) \
    .transform(lambda x: x.sum())
print(df4_join)
#                                  max_rating  sum_value status  total_all_status
# legal_entity counter_party tier
# L1           C1            1            3.0       40.0   ARAP              40.0
#              C3            3            6.0        5.0   ARAP               5.0
#              C4            4            6.0       40.0   ARAP             140.0
# L2           C2            2            2.0       20.0   ARAP              60.0
#              C3            3            NaN        0.0   ARAP              52.0
#              C5            5            6.0     1000.0   ARAP            1115.0
# L3           C3            3            NaN        0.0   ARAP             145.0
#              C6            6            5.0      145.0   ARAP             205.0
# L1           C1            1            NaN        0.0   ACCR              40.0
#              C3            3            NaN        0.0   ACCR               5.0
#              C4            4            5.0      100.0   ACCR             140.0
# L2           C2            2            3.0       40.0   ACCR              60.0
#              C3            3            2.0       52.0   ACCR              52.0
#              C5            5            4.0      115.0   ACCR            1115.0
# L3           C3            3            4.0      145.0   ACCR             145.0
#              C6            6            6.0       60.0   ACCR             205.0
