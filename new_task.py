import luigi
import subprocess
import random
import time

# Simple Task
class SimpleTask(luigi.Task):
    def run(self):
        with open('simple_task_output.txt', 'w') as f:
            f.write('Hello from SimpleTask!')

# Parameterized Task
class ParameterizedTask(luigi.Task):
    message = luigi.Parameter(default='Hello, World!')

    def run(self):
        with open('parameterized_task_output.txt', 'w') as f:
            f.write(self.message)

# Task with Dependencies
class TaskA(luigi.Task):
    def run(self):
        with open('task_a_output.txt', 'w') as f:
            f.write('Data from Task A')

class TaskB(luigi.Task):
    def requires(self):
        return TaskA()

    def run(self):
        with open('task_b_output.txt', 'w') as f:
            f.write('Data from Task B, depending on Task A')

# Task with File I/O
class ProcessDataTask(luigi.Task):
    input_file = luigi.Parameter()
    output_file = luigi.Parameter()

    def run(self):
        with open(self.input_file, 'w') as f:
            f.write('Sample data')
        
        # Simulating data processing
        time.sleep(2)
        
        with open(self.input_file, 'r') as infile:
            data = infile.read()
        
        with open(self.output_file, 'w') as outfile:
            outfile.write(f'Processed data: {data}')

# External Command Task
class ExternalCommandTask(luigi.Task):
    command = luigi.Parameter()

    def run(self):
        subprocess.run(self.command, shell=True, check=True)

# Task with Retry Logic
class RetryTask(luigi.Task):
    def run(self):
        if random.choice([True, False]):
            raise RuntimeError("Random failure!")
        with open('retry_task_success.txt', 'w') as f:
            f.write('Task succeeded')

    def on_failure(self, exception):
        print(f'Task failed with exception: {exception}')

# Workflow combining tasks
class FullWorkflow(luigi.Task):
    input_file = luigi.Parameter()
    output_file = luigi.Parameter()
    
    def requires(self):
        return [SimpleTask(), ParameterizedTask(message='Custom Message'), ProcessDataTask(input_file='input_data.txt', output_file='processed_data.txt')]

    def run(self):
        with open(self.output_file, 'w') as f:
            f.write('Full workflow completed')

if __name__ == '__main__':
    luigi.run()
