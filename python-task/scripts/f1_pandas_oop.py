import pandas as pd


##################################################################################################
class DataSource:
    def __init__(self, file_path):
        self.file_path = file_path

    def load_data(self):
        raise NotImplementedError("Subclasses must implement load_data")


class CSVDataSource(DataSource):
    def load_data(self):
        with open(self.file_path) as f:
            return pd.read_csv(f)


class JSONDataSource(DataSource):
    def load_data(self):
        with open(self.file_path) as f:
            return pd.read_json(f)


class DataTransformation:
    def __init__(self, transformation_function):
        self.transformation_function = transformation_function

    def transform(self, data):
        return self.transformation_function(data)


class DataSink:
    def __init__(self, output_path):
        self.output_path = output_path

    def save_data(self, data):
        data.to_csv(self.output_path, index=False)


class DataPipeline:
    def __init__(self, sources, transformations, sink):
        self.sources = sources
        self.transformations = transformations
        self.sink = sink

    def run_pipeline(self, merge_on):
        data = pd.merge(*[source.load_data() for source in self.sources], on=merge_on, how='inner')
        for transformation in self.transformations:
            data = transformation.transform(data)
        self.sink.save_data(data)


##################################################################################################
main_data_source = CSVDataSource("./data/dataset1.csv")
catalog_source = JSONDataSource("./data/dataset2.json")


def calculate_metrics(data):
    aggregation = {
        'count_rating': ('rating', 'count'),
        'max_tier': ('tier', 'max'),
        'sum_value': ('value', 'sum')
    }
    grouped_data = data.groupby(['legal_entity', 'counter_party']).agg(**aggregation).reset_index()
    return grouped_data


transformation_instance = DataTransformation(transformation_function=calculate_metrics)
data_sink = DataSink('./data/dataset_out.csv')

pipeline = DataPipeline([main_data_source, catalog_source], [transformation_instance], data_sink)
pipeline.run_pipeline(merge_on='counter_party')
