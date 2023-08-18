try:
    import os
    import uuid
    import sys
    import boto3
    import time
    import sys
    import json
    import pytz
    from pyspark.sql.functions import lit, udf
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from urllib.parse import urlparse
    from datetime import datetime, timedelta

except Exception as e:
    print("Modules are missing : {} ".format(e))


class ReadS3Uri():
    def __init__(self, s3_base_uri="", bookmark=False, AWS_ACCESS_KEY="", AWS_SECRET_KEY="", AWS_REGION="", bookmark_name="default_metadata", bookmark_path_uri=""):
        print(">> ReadS3Uri class INITIATED")
        self.bookmark_data = {}
        self.bookmark_name = bookmark_name
        self.bookmark_path_uri = bookmark_path_uri
        self.s3_base_uri = s3_base_uri
        self.source_bucket_name=""
        self.bookmark = bookmark
        self.s3_client = boto3.client(
            "s3",
            **self._get_credentials(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION) if AWS_ACCESS_KEY != "" else {}
        )

    def _get_credentials(self, AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_REGION):
        print("Using AWS Credentials")
        return {
            "aws_access_key_id": AWS_ACCESS_KEY,
            "aws_secret_access_key": AWS_SECRET_KEY,
            "region_name": AWS_REGION
        }

    def uri_parser(self, uri):
        parsed_uri = urlparse(uri)
        if parsed_uri.scheme != 's3':
            raise Exception(
                "Invalid URI scheme. Only 's3://' URIs are supported.")
        bucket_name = parsed_uri.netloc
        folder_path = parsed_uri.path.lstrip('/')
        return bucket_name, folder_path

    def upsert_bookmark(self, data, bookmark_path_uri="", bookmark_name="default_metadata"):
        try:
            bucket_name, folder_path = self.uri_parser(bookmark_path_uri)
            json_data = json.dumps(self.bookmark_data)
            self.s3_client.put_object(
                Bucket=bucket_name, Key=f"{folder_path}bookmark_metadata/{bookmark_name}.json", Body=json_data, ContentType='application/json')
            print(
                f">> Bookmark UPSERTED to '{bucket_name}/{folder_path}bookmark_metadata/{bookmark_name}.json' in '{bucket_name}'")
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def commit_bookmark(self):
        try:
            bucket_name, folder_path = self.uri_parser(self.bookmark_path_uri)
            json_data = json.dumps(self.bookmark_data)
            self.s3_client.put_object(
                Bucket=bucket_name, Key=f"{folder_path}bookmark_metadata/{self.bookmark_name}.json", Body=json_data, ContentType='application/json')
            print(
                f">> Bookmark COMMITED to '{bucket_name}/{folder_path}bookmark_metadata/{self.bookmark_name}.json' in '{bucket_name}'")
        except Exception as e:
            raise Exception("Error : {} ".format(e))

    def read_bookmark(self, bookmark_path_uri="", bookmark_name="default_metadata"):
        try:
            bucket_name, folder_path = self.uri_parser(bookmark_path_uri)
            response = self.s3_client.get_object(
                Bucket=bucket_name, Key=f"{folder_path}bookmark_metadata/{bookmark_name}.json")
            content = response['Body'].read().decode('utf-8')
            json_data = json.loads(content)
            print(
                f">> Bookmark FOUND | '{bucket_name}/{folder_path}bookmark_metadata/{bookmark_name}.json' in '{bucket_name}'")
            self.bookmark_data = json_data
            return json_data
        except Exception as e:
            print(">> Bookmark NOT FOUND | {} ".format(e))
            return {}

    def list_files_and_metadata(self):
        try:
            print(f">> reading files and metadata from URI : {self.s3_base_uri}")
            object_metadata_list = []
            parsed_uri = urlparse(self.s3_base_uri)
            if parsed_uri.scheme != 's3':
                raise Exception("Invalid URI scheme. Only 's3://' URIs are supported.")
            self.source_bucket_name = parsed_uri.netloc  # bucket_name
            folder_path = parsed_uri.path.lstrip('/')
            paginator = self.s3_client.get_paginator('list_objects')
            operation_parameters = {'Bucket': self.source_bucket_name, 'Prefix': folder_path}
            page_iterator = paginator.paginate(**operation_parameters)
            if self.bookmark:
                print(">> reading files with bookmark")
                self.read_bookmark(self.bookmark_path_uri, self.bookmark_name)
                if self.bookmark_data:
                    bookmark_timestamp = datetime.strptime(self.bookmark_data['upload_time'], "%Y-%m-%d %H:%M:%S")
                    bookmark_timestamp = pytz.utc.localize(bookmark_timestamp)  # Make it offset-aware
                    greatest_timestamp = bookmark_timestamp
                else:
                    bookmark_timestamp = datetime(1, 1, 1, tzinfo=pytz.UTC)  # Make it offset-aware
                    greatest_timestamp = bookmark_timestamp
                print(">> starting to read s3 files")
                for page in page_iterator:
                    for obj in page.get('Contents', []):
                        obj_key = obj['Key']
                        obj_metadata = self.s3_client.head_object(Bucket=self.source_bucket_name, Key=obj_key)
                        last_modified = obj_metadata['LastModified'].replace(tzinfo=pytz.UTC)  # Make it offset-aware
                        if last_modified > bookmark_timestamp:
                            object_metadata_list.append(obj['Key'])
                            greatest_timestamp = last_modified
                            self.bookmark_data = {
                                "file_name": obj['Key'],
                                'upload_time': last_modified.strftime("%Y-%m-%d %H:%M:%S")
                            }
                    
                    print(f"No of record read : {len(object_metadata_list)}")
            else:
                for page in page_iterator:
                    for obj in page.get('Contents', []):
                        object_metadata_list.append(obj['Key'])
                    print(f"No of record read : {len(object_metadata_list)}")
            return object_metadata_list
        except Exception as e:
            raise Exception(f"Error: {e}")

    def custom_uri_filter(self, file_paths, time_span=15):
        try:
            min_valid_timespan = timedelta(minutes=time_span)
            path_list = []

            for file_path in file_paths:
                parts = file_path.split('-')
                if len(parts) == 3:
                    date = parts[1] + '-' + parts[2].replace('.csv', '')
                    if "conv" in date:
                        continue
                    key_parts = date.rsplit('-', 1)
                    try:
                        start_timestamp = datetime.strptime(
                            key_parts[0], '%Y%m%dT%H%M')
                        end_timestamp = datetime.strptime(
                            key_parts[1], '%Y%m%dT%H%M')
                    except ValueError:
                        print(
                            f"Skipping {file_path}: Invalid timestamp format")
                        continue

                    if end_timestamp.date() == start_timestamp.date() and end_timestamp - start_timestamp >= min_valid_timespan:
                        path_list.append(f"s3://{self.source_bucket_name}/{file_path}")
                        # print(f"Processing {file_path}")
                    else:
                        # print(f"Skipping {file_path}")
                        pass
                else:
                    pass
            print(">> applied custom filter SUCCESS")
            return path_list
        except Exception as e:
            raise Exception(f"ERROR: {e}")

    def handler(self):
        uri_list=self.list_files_and_metadata()
        final_uri_list = self.custom_uri_filter(uri_list)
        print(f"new records : {len(final_uri_list)}")
        return final_uri_list