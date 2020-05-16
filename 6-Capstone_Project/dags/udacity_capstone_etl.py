from datetime import datetime, timedelta
import os
import shutil
import logging
import s3fs
import configparser
import re
import pandas as pd
from plugins.helpers import aws_connections
# airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

config = configparser.ConfigParser()
config.read('./plugins/helpers/dwh_airflow.cfg')

aws_connect = aws_connections.get_aws_access_id(aws_connections.AirflowConnectionIds.S3)

PARAMS = {'aws_access_key': aws_connect.get('aws_access_key'),
          'aws_secret': aws_connect.get('aws_secret'),
          'FINAL_DATA_BUCKET' : config.get('S3', 'FINAL_DATA_BUCKET'),
          'RAW_DATA_BUCKET' : config.get('S3', 'RAW_DATA_BUCKET'),
          'VISA_DATA_LOC' : config.get('S3', 'VISA_DATA'),
          'CODES_DATA_LOC' : config.get('S3','CODES_DATA'),
          'SAS_LABELS_DATA_LOC' : config.get('S3','SAS_LABELS_DATA'),
          'I94_RAW_DATA_LOC' : config.get('S3','I94_RAW_DATA'),
          'DEMOGRAPHICS_DATA_LOC' : config.get('S3','DEMOGRAPHICS_DATA')
          }


def sas_labels_to_csv(*args, **kwargs):
    s3 = s3fs.S3FileSystem(anon=False,
                           key=PARAMS['aws_access_key'],
                           secret=PARAMS['aws_secret'])

    with s3.open(PARAMS['RAW_DATA_BUCKET'] + PARAMS['SAS_LABELS_DATA_LOC'] +
                 'I94_SAS_Labels_Descriptions.SAS') as i94_description:
        i94_label_content = i94_description.read()

    data_dict = {}
    df_dict = {}
    key_name = ''
    for line in i94_label_content.split("\n"):
        line = re.sub(r"\s+", " ", line)
        if '/*' in line and '-' in line:
            line = line.strip('/*')
            key_name = line.split('-')[0].strip()
            data_dict[key_name] = []
        if '=' in line and key_name != '':
            data_dict[key_name].append(
                [item.strip(';').strip(" ").replace('\'', '').lstrip().rstrip() for item in line.split('=')])

    for key in data_dict.keys():
        if len(data_dict[key]) > 0:
            if 'CIT' in key and 'RES' in key:
                i94cit_i94res = pd.DataFrame(data_dict[key], columns=['i94_country_code', 'country_name'])
                i94cit_i94res.loc[i94cit_i94res.country_name.str.contains('MEXICO'), 'country_name'] = 'MEXICO'
                df_dict['i94cit_i94res'] = i94cit_i94res
            if 'PORT' in key:
                i94port_i94code = pd.DataFrame(data_dict[key], columns=['i94_port_code', 'i94_airport_location'])
                i94port_i94code[['port_city', 'port_state']] = i94port_i94code['i94_airport_location'].str.rsplit(',',1,expand=True)
                i94port_i94code.drop(['i94_airport_location'], axis=1, inplace=True)
                df_dict['i94port_i94code'] = i94port_i94code
            if 'MODE' in key:
                i94mode = pd.DataFrame(data_dict[key], columns=['i94_mode_code', 'i94_mode'])
                df_dict['i94mode'] = i94mode
            if 'ADDR' in key:
                i94addr = pd.DataFrame(data_dict[key], columns=['i94_state_code', 'i94_state_name'])
                df_dict['i94addr'] = i94addr
            if 'VISA' in key:
                i94visa = pd.DataFrame(data_dict[key], columns=['i94_visa_code', 'visa_purpose'])
                df_dict['i94visa'] = i94visa

    for key in df_dict.keys():
        logging.info(f"Writing {key} Table to Final S3 Bucket")
        with s3.open(f"{PARAMS['FINAL_DATA_BUCKET']}/i94_meta_data/{key}.csv", "w") as f:
            df_dict[key].to_csv(f, index=False)

