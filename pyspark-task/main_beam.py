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
            yield (legal_entity, counter_party, status), (tier, rating, value)
            yield (legal_entity, status), (counter_party, tier, rating, value)
            yield (counter_party, status), (legal_entity, tier, rating, value)
            yield (tier, status), (legal_entity, counter_party, rating, value)
            yield (legal_entity, tier, status), (counter_party, rating, value)
            yield (counter_party, tier, status), (legal_entity, rating, value)


class CalculateCountMaxSum(beam.CombineFn):
    def create_accumulator(self):
        return (0, 0, 0, 0)

    def add_input(self, accumulator, element):
        if len(element) == 3:
            count, max_tier, total_value, _ = accumulator
            col_for_count, rating, value = element
            count += 1
            max_tier = max(max_tier, rating)
            total_value += value
            return count, max_tier, total_value, 0
        else:
            count1, count2, max_tier, total_value = accumulator
            col1_for_count, col2_for_count, rating, value = element
            count1 += 1
            count2 += 1
            max_tier = max(max_tier, rating)
            total_value += value
            return count1, count2, max_tier, total_value

    def merge_accumulators(self, accumulators):
        if len(accumulators[0]) == 3:
            total_count, max_tier, total_value = zip(*accumulators)
            return sum(total_count), max(max_tier), sum(total_value), 0
        else:
            total_count1, total_count2, max_tier, total_value = zip(*accumulators)
            return sum(total_count1), sum(total_count2), max(max_tier), sum(total_value)

    def extract_output(self, accumulator):
        return accumulator


sum_arap = (
        merged_dataset
        | 'arap sum' >> beam.ParDo(GroupByCols('ARAP'))
        | 'get arap sum' >> beam.CombinePerKey(CalculateCountMaxSum())
)

sum_accr = (
        merged_dataset
        | 'accr sum' >> beam.ParDo(GroupByCols('ACCR'))
        | 'get accr sum' >> beam.CombinePerKey(CalculateCountMaxSum())
)

sum_results = (
        (sum_arap, sum_accr)
        | beam.Flatten()
)

# Print the merged and aggregated data
sum_results | 'Print aggregated data' >> beam.Map(print)

pipeline.run()
# (('L1', 'C1', 'ARAP'), (3, 3, 40, 0))
# (('L1', 'ARAP'), (5, 5, 6, 85))
# (('C1', 'ARAP'), (3, 3, 3, 40))
# ((1, 'ARAP'), (3, 3, 3, 40))
# (('L1', 1, 'ARAP'), (3, 3, 40, 0))
# (('C1', 1, 'ARAP'), (3, 3, 40, 0))
# (('L2', 'C2', 'ARAP'), (1, 2, 20, 0))
# (('L2', 'ARAP'), (2, 2, 6, 1020))
# (('C2', 'ARAP'), (1, 1, 2, 20))
# ((2, 'ARAP'), (1, 1, 2, 20))
# (('L2', 2, 'ARAP'), (1, 2, 20, 0))
# (('C2', 2, 'ARAP'), (1, 2, 20, 0))
# (('L1', 'C4', 'ARAP'), (1, 6, 40, 0))
# (('C4', 'ARAP'), (1, 1, 6, 40))
# ((4, 'ARAP'), (1, 1, 6, 40))
# (('L1', 4, 'ARAP'), (1, 6, 40, 0))
# (('C4', 4, 'ARAP'), (1, 6, 40, 0))
# (('L2', 'C5', 'ARAP'), (1, 6, 1000, 0))
# (('C5', 'ARAP'), (1, 1, 6, 1000))
# ((5, 'ARAP'), (1, 1, 6, 1000))
# (('L2', 5, 'ARAP'), (1, 6, 1000, 0))
# (('C5', 5, 'ARAP'), (1, 6, 1000, 0))
# (('L3', 'C6', 'ARAP'), (2, 5, 145, 0))
# (('L3', 'ARAP'), (2, 2, 5, 145))
# (('C6', 'ARAP'), (2, 2, 5, 145))
# ((6, 'ARAP'), (2, 2, 5, 145))
# (('L3', 6, 'ARAP'), (2, 5, 145, 0))
# (('C6', 6, 'ARAP'), (2, 5, 145, 0))
# (('L1', 'C3', 'ARAP'), (1, 6, 5, 0))
# (('C3', 'ARAP'), (1, 1, 6, 5))
# ((3, 'ARAP'), (1, 1, 6, 5))
# (('L1', 3, 'ARAP'), (1, 6, 5, 0))
# (('C3', 3, 'ARAP'), (1, 6, 5, 0))
# (('L3', 'C3', 'ACCR'), (3, 4, 145, 0))
# (('L3', 'ACCR'), (4, 4, 6, 205))
# (('C3', 'ACCR'), (4, 4, 4, 197))
# ((3, 'ACCR'), (4, 4, 4, 197))
# (('L3', 3, 'ACCR'), (3, 4, 145, 0))
# (('C3', 3, 'ACCR'), (4, 4, 197, 0))
# (('L2', 'C5', 'ACCR'), (2, 4, 115, 0))
# (('L2', 'ACCR'), (4, 4, 4, 207))
# (('C5', 'ACCR'), (2, 2, 4, 115))
# ((5, 'ACCR'), (2, 2, 4, 115))
# (('L2', 5, 'ACCR'), (2, 4, 115, 0))
# (('C5', 5, 'ACCR'), (2, 4, 115, 0))
# (('L3', 'C6', 'ACCR'), (1, 6, 60, 0))
# (('C6', 'ACCR'), (1, 1, 6, 60))
# ((6, 'ACCR'), (1, 1, 6, 60))
# (('L3', 6, 'ACCR'), (1, 6, 60, 0))
# (('C6', 6, 'ACCR'), (1, 6, 60, 0))
# (('L2', 'C2', 'ACCR'), (1, 3, 40, 0))
# (('C2', 'ACCR'), (1, 1, 3, 40))
# ((2, 'ACCR'), (1, 1, 3, 40))
# (('L2', 2, 'ACCR'), (1, 3, 40, 0))
# (('C2', 2, 'ACCR'), (1, 3, 40, 0))
# (('L1', 'C4', 'ACCR'), (1, 5, 100, 0))
# (('L1', 'ACCR'), (1, 1, 5, 100))
# (('C4', 'ACCR'), (1, 1, 5, 100))
# ((4, 'ACCR'), (1, 1, 5, 100))
# (('L1', 4, 'ACCR'), (1, 5, 100, 0))
# (('C4', 4, 'ACCR'), (1, 5, 100, 0))
# (('L2', 'C3', 'ACCR'), (1, 2, 52, 0))
# (('L2', 3, 'ACCR'), (1, 2, 52, 0))
