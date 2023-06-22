import re

import apache_beam as beam


# In this solution, we used the Pardo function to process the join:
# dataset2 is used as a dictionary (sideinput) to join with dataset1.
# The output of the join is flattened and the desired output is generated.
class ProcessMainDataSet(beam.DoFn):
    def process(self, element, dataset2_dict):
        invoice_id, legal_entity, counter_party, rating, status, value = element
        if counter_party in dataset2_dict:
            yield counter_party, (
                invoice_id, legal_entity, counter_party, int(dataset2_dict[counter_party]), int(rating), status,
                int(value))


class ProcessJoiningDataset(beam.DoFn):
    def process(self, element):
        counter_party, tier = element
        yield counter_party, tier


# Define a pipeline
pipeline = beam.Pipeline()

# Read the data from the CSV files and join them
dataset2 = (
        pipeline
        | 'Read dataset2' >> beam.io.ReadFromText('dataset2.csv')
        | 'Get each record' >> beam.Map(lambda line: re.sub(r'\r', ',', line).split(',')[2:])
        | 'Group records based on len(col)=2' >> beam.FlatMap(
    lambda array: [array[i:i + 2] for i in range(0, len(array), 2)])
        | 'Process dataset2' >> beam.ParDo(ProcessJoiningDataset())
)

merged_dataset = (
        pipeline
        | 'Read dataset1' >> beam.io.ReadFromText('dataset1.csv')
        | 'Get each record ready' >> beam.Map(lambda line: re.sub(r'\r', ',', line).split(',')[6:])
        | 'Group cells into rows' >> beam.FlatMap(lambda array: [array[i:i + 6] for i in range(0, len(array), 6)])
        | 'Process dataset1 and merge dataset2 in' >> beam.ParDo(ProcessMainDataSet(), beam.pvalue.AsDict(dataset2))
)


# Define all the group by functions
class GroupByCols(beam.DoFn):
    def __init__(self, status):
        self.status = status

    def process(self, element):
        counter_party, (
            invoice_id, legal_entity, counter_party, tier, rating, status, value) = element
        if status == self.status:
            yield (legal_entity, counter_party, tier, status), value
            yield (legal_entity, status), (counter_party, tier, value)
            yield (counter_party, status), (legal_entity, tier, value)
            yield (tier, status), (legal_entity, counter_party, value)
            yield (legal_entity, counter_party, status), (tier, value)
            yield (legal_entity, tier, status), (counter_party, value)
            yield (counter_party, tier, status), (legal_entity, value)


class CalculateCountMaxSum(beam.CombineFn):
    def create_accumulator(self):
        return (0, float('-inf'), 0)

    def add_input(self, accumulator, element):
        count, max_tier, total_value = accumulator
        col_for_count, col_for_max, value = element
        count += 1
        max_tier = max(max_tier, col_for_max)
        total_value += value
        return count, max_tier, total_value

    def merge_accumulators(self, accumulators):
        total_count, max_tier, total_value = zip(*accumulators)
        return sum(total_count), max(max_tier), sum(total_value)

    def extract_output(self, accumulator):
        return accumulator


sum_arap = (
        merged_dataset
        | 'arap sum' >> beam.ParDo(GroupByCols('ARAP'))
    # | 'get arap sum' >> beam.CombinePerKey(CalculateCountMaxSum())
)

sum_accr = (
        merged_dataset
        | 'accr sum' >> beam.ParDo(GroupByCols('ACCR'))
    # | 'get accr sum' >> beam.CombinePerKey(CalculateCountMaxSum())
)

sum_results = (
        (sum_arap, sum_accr)
        | beam.Flatten()
)

# Print the merged and aggregated data
sum_results | 'Print aggregated data' >> beam.Map(print)

pipeline.run()
