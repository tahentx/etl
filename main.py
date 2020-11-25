import luigi

class SayHello(luigi.Task):
    def output(self):
        return luigi.LocalTarget('result.csv')
    def run(self):
        print('hello world')
        with self.output().open('w') as f:
            f.write('ok')

def print_hi(name):
    print("hi " + str(name))
if __name__ == '__main__':
    print(print_hi("Todd"))
