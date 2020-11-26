import luigi

class SayHello(luigi.Task):
    def output(self):
        return luigi.LocalTarget('result.csv')
    def run(self):
        print('hello world')
        with self.output().open('w') as f:
            f.write('ok')

# class MyTask(luigi.Task):
#     x = luigi.IntParameter()
#     y = luigi.IntParameter(default=45)

#     def run(self):
#         print(self.x + self.y)


def print_hi(name):
    print("hi " + str(name))
if __name__ == '__main__':
    luigi.run(['SayHello', '--local-scheduler'])
