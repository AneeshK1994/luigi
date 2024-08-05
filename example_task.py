import luigi

class HelloWorldTask(luigi.Task):
    def run(self):
        print("Hello, World!")

if __name__ == '__main__':
    luigi.run()
