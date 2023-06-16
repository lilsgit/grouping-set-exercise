# Framework 2: beam

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def split_by_kv(element, index, delimiter=","):
    # Need a better approach here
    split = element.split(delimiter)
    return split[index], element


# Read the data from the csv file
with beam.Pipeline(options=PipelineOptions()) as p:
    df1 = p \
          | 'Read dataset1' >> beam.io.ReadFromText('dataset1.csv', skip_header_lines=0) \
          | 'Split by counter_party at index 2' >> beam.Map(split_by_kv, 2)

    df2 = p \
          | 'Read dataset2' >> beam.io.ReadFromText('dataset2.csv', skip_header_lines=0) \
          | 'Split by counter_party at index 0' >> beam.Map(split_by_kv, 0)

    # Merge the two dataframes
    df3_first_merge = ({"df1": df1, "df2": df2}
                       | beam.CoGroupByKey()
                       )


    # Generate desired output
    # legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP),
    # sum(value where status=ACCR)
    def process_data(element):
        counter_party, data = element
        df1_values = data["df1"][0].split("\r")[1:]
        df2_values = data["df2"][0].split("\r")[1:]

        for i in range(len(df1_values)):
            split_string = df1_values[i].split(',')
            key = split_string[2]  # Extract the key from the split string
            for pair in df2_values:
                if pair.startswith(key + ','):  # Check if the key exists in the key-value pairs
                    value = pair.split(',')[1]  # Extract the corresponding value
                    df1_values[i] += "," + value

        max_ratings = {}
        sum_by_status = {}
        total = {}
        for value in df1_values:
            split_value = value.split(',')
            legal_entity = split_value[1]
            counter_party = split_value[2]
            tier = split_value[6]
            status = split_value[4]
            key = (legal_entity, counter_party, tier, status)
            rating = int(split_value[3])

            if key in max_ratings:
                max_ratings[key] = max(max_ratings[key], rating)
            else:
                max_ratings[key] = rating

            if key in sum_by_status:
                sum_by_status[key] += int(split_value[5])
            else:
                sum_by_status[key] = int(split_value[5])

            combined_dict = {}
            for key, value in max_ratings.items():
                if key in sum_by_status:
                    combined_dict[key] = (value, sum_by_status[key])

            # create new record to add total for each of legal entity, counterparty & tier.
            new_key = (legal_entity, counter_party, tier)
            if new_key in total:
                total[new_key] += int(split_value[5])
            else:
                total[new_key] = int(split_value[5])

            # add total to combined_dict
            final_combined_dict = {}
            for key1, value1 in total.items():
                for key2, value2 in combined_dict.items():
                    if key1[:3] == key2[:3]:
                        combined_key = key1 + key2[3:]
                        final_combined_dict[combined_key] = (value2[0], value2[1], value1)

        output = "legal_entity,counterparty,tier,status,max_rating,sum_by_status,total\n"
        for key, (max_rating, sum_by_status, total) in final_combined_dict.items():
            legal_entity, counterparty, tier, status = key
            output += f"{legal_entity},{counterparty},{tier},{status},{max_rating},{sum_by_status},{total}\n"

        return output


    df4 = df3_first_merge \
          | beam.Map(process_data) \
          | beam.io.WriteToText('output.csv')

# legal_entity,counterparty,tier,status,max_rating,sum_by_status,total
# L1,C1,1,ARAP,3,40,40
# L2,C2,2,ARAP,2,20,60
# L2,C2,2,ACCR,3,40,60
# L3,C3,3,ACCR,4,145,145
# L1,C4,4,ARAP,6,40,140
# L1,C4,4,ACCR,5,100,140
# L2,C5,5,ACCR,4,115,1115
# L2,C5,5,ARAP,6,1000,1115
# L3,C6,6,ACCR,6,60,205
# L3,C6,6,ARAP,5,145,205
# L2,C3,3,ACCR,2,52,52
# L1,C3,3,ARAP,6,5,5
