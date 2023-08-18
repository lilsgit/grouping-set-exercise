from dataclasses import dataclass

import pandas as pd


@dataclass
class InvoiceData:
    invoice_id: int
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    value: int


def read_csv(file_path):
    return pd.read_csv(file_path, on_bad_lines='warn')


def validate_data(dataset):
    invalid_rows = []

    for index, row in dataset.iterrows():
        try:
            InvoiceData(
                int(row['invoice_id']),
                row['legal_entity'],
                row['counter_party'],
                int(row['rating']),
                row['status'],
                int(row['value'])
            )
        except (ValueError, KeyError) as e:
            invalid_rows.append((index + 1, e))

    return invalid_rows


df = read_csv('./data/dataset1_val.csv')
validation_result = validate_data(df)
print(validation_result)
