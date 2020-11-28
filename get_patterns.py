import luigi
import os
from luigi import Task, LocalTarget
from luigi.contrib.s3 import S3Client, S3Target
# aws s3 ls s3://sg-c19-response/weekly-patterns-delivery/weekly/ --profile safegraphws --endpoint https://s3.wasabisys.com
class GetWeeklyPatterns(Task):
    def output(self):
        return LocalTarget('patterns.csv')
    def run(self):
        S3Client.get('s3://sg-c19-response/weekly-patterns-delivery/weekly/', os.path.dirname())

class UploadS3(Task):
    def requires(self):
        return GetWeeklyPatterns()
    def output(self):
        return S3Target('https://pluralsightpractice/patterns.csv')
    
    def run(self):
        with self.output().open('w') as f:
            f.write("to do...")

if __name__ == '__main__':
    luigi.run(['UploadS3','--local-scheduler'])

