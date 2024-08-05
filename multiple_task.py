import luigi
import pandas as pd

# Task 1: Generate Sample Data
class GenerateData(luigi.Task):
    def output(self):
        return luigi.LocalTarget('data.csv')

    def run(self):
        # Generate some sample data
        data = pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })
        data.to_csv(self.output().path, index=False)

# Task 2: Transform Data
class TransformData(luigi.Task):
    def requires(self):
        return GenerateData()

    def output(self):
        return luigi.LocalTarget('transformed_data.csv')

    def run(self):
        # Read data from the output of GenerateData
        data = pd.read_csv(self.input().path)
        # Add a new column with transformed data
        data['transformed'] = data['value'].apply(lambda x: x.upper())
        data.to_csv(self.output().path, index=False)

# Task 3: Aggregate Data
class AggregateData(luigi.Task):
    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget('aggregated_data.txt')

    def run(self):
        # Read data from the output of TransformData
        data = pd.read_csv(self.input().path)
        # Perform aggregation (e.g., count unique values)
        unique_count = data['transformed'].nunique()
        with open(self.output().path, 'w') as f:
            f.write(f'Unique transformed values count: {unique_count}')

if __name__ == '__main__':
    luigi.run()
