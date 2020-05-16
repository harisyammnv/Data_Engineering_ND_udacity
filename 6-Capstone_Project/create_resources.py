import boto3
import configparser
import os
from AWS_cf_stack_provider import *

config = configparser.ConfigParser()
config.read('./dwh.cfg')

AWS_ACCESS_KEY = config.get('AWS', 'AWS_KEY_ID')
AWS_SECRET = config.get('AWS', 'AWS_SECRET')
AWS_REGION = config.get('AWS', 'REGION')
AWS_EC2_KEY_PAIR = config.get('AWS', 'AWS_EC2_KEY_PAIR')
TEMPLATE_URL = config.get('S3', 'CF_TEMPLATE_URL')
FINAL_DATA_BUCKET = config.get('S3', 'FINAL_DATA_BUCKET')
RAW_DATA_BUCKET = config.get('S3', 'RAW_DATA_BUCKET')
create_stack = False
upload_files = False

if upload_files:
    s3_client = boto3.client('s3', region_name=AWS_REGION,
                             aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET)

    sas_data = "./data/18-83510-I94-Data-2016/"
    files = [sas_data + f for f in os.listdir(sas_data)]
    for f in files:
        s3_client.upload_file(f, RAW_DATA_BUCKET, "/raw/i94_immigration_data/" + f.split("/")[-1])

    s3_client.upload_file("data/us-cities-demographics.csv", RAW_DATA_BUCKET,
                          "raw/us-demographics/us-cities-demographics.csv")
    s3_client.upload_file("data/nationality-codes.csv", RAW_DATA_BUCKET,
                          "raw/codes/nationality-codes.csv")
    s3_client.upload_file("data/port-of-entry-codes.csv", RAW_DATA_BUCKET,
                          "raw/codes/port-of-entry-codes.csv")
    s3_client.upload_file("data/airport-codes.csv", RAW_DATA_BUCKET,
                          "raw/codes/airport-codes.csv")
    s3_client.upload_file("data/visa-issuing-ports.csv", RAW_DATA_BUCKET,
                          "raw/us-visa/visa-issuing-ports.csv")
    s3_client.upload_file("data/visa-type.csv", RAW_DATA_BUCKET,
                          "raw/us-visa/visa-type.csv")
    s3_client.upload_file("data/I94_SAS_Labels_Descriptions.SAS", RAW_DATA_BUCKET,
                          "raw/sas_meta_data/I94_SAS_Labels_Descriptions.SAS")

if create_stack:
    aws_stack_provider = AWSCloudFormationStackProvider(aws_key=AWS_ACCESS_KEY, aws_secret=AWS_SECRET,
                                                        key_pair=AWS_EC2_KEY_PAIR, region=AWS_REGION,
                                                        template_url=TEMPLATE_URL, final_bucket=FINAL_DATA_BUCKET)
    cf_client = aws_stack_provider.get_cloud_formation_client()
    aws_s3_client = aws_stack_provider.get_s3_client()
    valid_stack_template = aws_stack_provider.get_stack_template()
    if valid_stack_template:
        aws_stack_provider.create_stack(stack_name='DEND-Stack')

    if aws_stack_provider.stack_exists(stack_name='DEND-Stack'):
        print('Cloud Formation Stack Exists')
