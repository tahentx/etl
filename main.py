import luigi
from luigi import Task, LocalTarget
# class SayHello(Task):
#     def output(self):
#         return luigi.LocalTarget('result.csv')
#     def run(self):
#         print('hello world')
#         with self.output().open('w') as f:
#             f.write('ok')

class ProcessOrders(Task):
    def output(self):
        return LocalTarget('orders.csv')
    def run(self):
        with self.output().open('w') as f:
            print('May, 100', file=f)
            print('May, 180', file=f)
            print('June, 200', file=f)
            print('June, 150', file=f)

class GenerateReport(Task):
    def requires(self):
        return ProcessOrders()
    def output(self):
        return LocalTarget('report.csv')
    def run(self):
        report = {}
        for line in self.input().open():
            month, amount = line.split(',')
            if month in report:
                report[month] += float(amount)
            else:
                report[month] = float(amount)
        with self.output().open('w') as out:
            for month in report:
                print(month + ',' + str(report[month]), file=out)

class SummarizeReport(Task):
    def requires(self):
        return GenerateReport()
    def output(self):
        return LocalTarget('summary.txt')
    def run(self):
        total = 0.0
        for line in self.input().open():
            month, amount = line.split(',')
            total += float(amount)
        with self.output().open('w') as f:
            f.write(str(total))
# class MyTask(luigi.Task):
#     x = luigi.IntParameter()
#     y = luigi.IntParameter(default=45)

#     def run(self):
#         print(self.x + self.y)

if __name__ == '__main__':
    luigi.run(['SummarizeReport'])
