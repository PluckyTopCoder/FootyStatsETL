from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from glob import glob
import boto3


class S3UploadOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 region="",
                 aws_key="",
                 aws_secret="",
                 bucket="",
                 s3_key="",
                 local_path="",
                 *args, **kwargs):

        super(S3UploadOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.region = region
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.bucket = bucket
        self.s3_key = s3_key
        self.local_path = local_path

    def execute(self, context):
        # Clear old files from S3 folder
        session = boto3.Session(
            aws_access_key_id=self.aws_key,
            aws_secret_access_key=self.aws_secret,
            region_name=self.region)
        
        s3_resource = session.resource('s3')
        bucket = s3_resource.Bucket(self.bucket)
        
        for obj in bucket.objects.filter(Prefix=self.s3_key):
            obj.delete()
        
        
        # Upload local files to S3 bucket
        s3_client = boto3.client('s3', region_name=self.region, 
                           aws_access_key_id=self.aws_key,
                          aws_secret_access_key=self.aws_secret)
        
        
        files = glob(self.local_path + f"*.json")
        
        for file in files:
            file_name = file[len(self.local_path):]
            self.log.info(file_name)
            s3_client.upload_file(file, self.bucket, self.s3_key + file_name)

