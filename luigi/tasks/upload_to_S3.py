import luigi

#import convert_foodreview_data
from luigi import configuration, s3
from luigi.s3 import S3Target, S3Client

from boto.s3.key import Key
from boto.s3.connection import S3Connection
class UploadDataToS3(luigi.Task):


    aws_access_key_id = "AKIAIFBIIN7FQAQ2KZPA"

    aws_secret_access_key = "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"

    def requires(self):
        return []

    def input(self):
        return luigi.LocalTarget('C:/Users/Admin/Documents/DataScience/FinalProject/data/Reviews.csv')
    def run(self):
        conn=S3Connection(self.aws_access_key_id,self.aws_secret_access_key)
        bucket = conn.create_bucket("team11_data")
        k=Key(bucket)
        k.key = 'Reviews.csv' # to-do $$$$
        k.set_contents_from_string(self.input().path) # to-do $$$$
        print('uploading to S3')

if __name__ == "__main__":

	luigi.run()
