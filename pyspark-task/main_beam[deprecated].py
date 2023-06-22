import re

import apache_beam as beam

# Define a pipeline
pipeline = beam.Pipeline()

# Read the data from the CSV files
dataset1 = (
        pipeline
        | 'Read dataset1' >> beam.io.ReadFromText('dataset1.csv')
        | 'Get each line' >> beam.Map(lambda line: re.sub(r'\r', ',', line).split(',')[6:])
        | 'Group records' >> beam.Map(lambda array: [array[i:i + 6] for i in range(0, len(array), 6)])
        | 'Create key-value pairs' >> beam.FlatMap(lambda fields: [(item[2], {
    'invoice_id': item[0],
    'legal_entity': item[1],
    'counter_party': item[2],
    'rating': item[3],
    'status': item[4],
    'value': item[5]
}) for item in fields])
)

dataset2 = (
        pipeline
        | 'Read dataset2' >> beam.io.ReadFromText('dataset2.csv')
        | 'Get each line for d2' >> beam.Map(lambda line: re.sub(r'\r', ',', line).split(',')[2:])
        | 'Group records for d2' >> beam.Map(lambda array: [array[i:i + 2] for i in range(0, len(array), 2)])
        | 'Create key-value pairs for d2' >> beam.FlatMap(lambda fields: [(item[0], {
    'counter_party': item[0],
    'tier': item[1],
}) for item in fields])
)

# Merge the two PCollections
merged_data = (
        ({'df1': dataset1, 'df2': dataset2})
        | 'Merge datasets based on counter_party' >> beam.CoGroupByKey()
        | 'Flatten' >> beam.FlatMap(lambda data: [{**{'legal_entity': d1['legal_entity'],
                                                      'counter_party': d1['counter_party'],
                                                      'tier': data[1]['df2'][0]['tier'], 'rating': d1['rating'],
                                                      'status': d1['status'], 'value': d1['value']},
                                                   **data[1]['df2'][0]} for d1 in data[1]['df1']]
                                    )
)

# Aggregate the data to generate the desired output
aggregated_data = (
        merged_data
        | 'Add max_rating, sum_of_ARAP, and sum_of_ACCR' >> beam.Map(lambda data: (
    (data['legal_entity'], data['counter_party']),
    {
        'legal_entity': data['legal_entity'],
        'counter_party': data['counter_party'],
        'tier': data['tier'],
        'max_rating': data['rating'],
        'sum_of_ARAP': int(data['value']) if data['status'] == 'ARAP' else 0,
        'sum_of_ACCR': int(data['value']) if data['status'] == 'ACCR' else 0
    }
))
        | 'Group by legal_entity and counter_party' >> beam.GroupByKey()
        | 'Calculate max_rating, sum_of_ARAP, and sum_of_ACCR' >> beam.Map(lambda data: {
    'legal_entity': data[0][0],
    'counter_party': data[0][1],
    'tier': max(entry['tier'] for entry in data[1]),
    'max_rating': max(entry['max_rating'] for entry in data[1]),
    'sum_of_ARAP': sum(entry['sum_of_ARAP'] for entry in data[1]),
    'sum_of_ACCR': sum(entry['sum_of_ACCR'] for entry in data[1])
})
)

# Define the header for the output
header = 'legal_entity counter_party  tier  max_rating  sum_of_ARAP  sum_of_ACCR'

# Print the header
print(header)

# Print the formatted output
formatted_output = (
        aggregated_data
        | 'Format output' >> beam.Map(lambda data: (
    data['legal_entity'],
    data['counter_party'],
    data['tier'],
    data['max_rating'],
    data['sum_of_ARAP'],
    data['sum_of_ACCR']
))
        | 'Format and print rows' >> beam.Map(lambda data: '           '.join(str(value) for value in data))
        | 'Print rows' >> beam.Map(print)
)

pipeline.run()
# legal_entity counter_party  tier  max_rating  sum_of_ARAP  sum_of_ACCR
# L1           C1           1           3           40           0
# L2           C2           2           3           20           40
# L3           C3           3           4           0           145
# L2           C3           3           2           0           52
# L1           C3           3           6           5           0
# L1           C4           4           6           40           100
# L2           C5           5           6           1000           115
# L3           C6           6           6           145           60
