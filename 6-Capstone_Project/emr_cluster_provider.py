import boto3
import configparser


class EMRClusterProvider:

    def __init__(self, aws_key, aws_secret, region, num_nodes):
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region
        self.num_nodes = num_nodes

    def create_client(self):
        emr = boto3.client('emr', region_name=self.region)
        return emr
