import boto3
import time
import json
import psycopg2
from upsert import Upsert


class Worker(object):
    # AWS resources properties
    S3_BUCKET_NAME = 'transaction-files'
    SQS_QUEUE_NAME = 'transaction-queue'
    S3_ENDPOINT = 'http://mocks3:9444/s3'
    SQS_ENDPOINT = 'http://mocksqs:9324'
    SQS_QUEUE_URL = 'http://mocksqs:9324/queue/transaction-queue'

    # Below are mock credentials and not any specific accounts keys
    AWS_ACCESS_KEY = 'AKIAIOSFODNN7EXAMPLE'
    AWS_SECRET_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

    # Redshift connection params
    REDSHIFT_HOST = 'targetdb'
    REDSHIFT_DB = 'postgres'
    REDSHIFT_USER = 'postgres'
    REDSHIFT_PASSWORD = 'password'
    REDSHIFT_PORT = 5432
    STAGING_TABLE = 'staging.transactions'
    TARGET_TABLE = 'public.transactions'

    def __init__(self):
        self.s3_client = self.init_s3_connection()
        self.sqs_client = self.init_sqs_connection()
        self.redshift_conn = self.init_redshift_connection()
        self.create_aws_resources()
        self.add_bucket_notification()

    def init_s3_connection(self):
        """
        :return: S3 client connection
        """
        s3 = boto3.client('s3',
                          aws_access_key_id=self.AWS_ACCESS_KEY,
                          aws_secret_access_key=self.AWS_SECRET_KEY,
                          endpoint_url=self.S3_ENDPOINT,
                          region_name='eu-west-1'
                          )
        return s3

    def init_sqs_connection(self):
        """
        :return: SQS client connection
        """
        sqs = boto3.client('sqs',
                           aws_access_key_id=self.AWS_ACCESS_KEY,
                           aws_secret_access_key=self.AWS_SECRET_KEY,
                           endpoint_url=self.SQS_ENDPOINT,
                           region_name='eu-west-1'
                           )
        return sqs

    def init_redshift_connection(self):
        """
        :return: Redshift connection
        """
        conn = psycopg2.connect(host=self.REDSHIFT_HOST,
                                dbname=self.REDSHIFT_DB,
                                user=self.REDSHIFT_USER,
                                password=self.REDSHIFT_PASSWORD,
                                port=self.REDSHIFT_PORT
                                )
        return conn

    def create_aws_resources(self):
        """
        Creates S3 and SQS queues that are required
        :return: None
        """
        s3 = self.s3_client
        s3.create_bucket(Bucket=self.S3_BUCKET_NAME)

        sqs = self.sqs_client
        sqs.create_queue(QueueName=self.SQS_QUEUE_NAME)

    def add_bucket_notification(self):
        """
        Adds S3 bucket notification to send events to SQS queue when a new object is uploaded to the bucket
        :return: None
        """
        data = dict()
        data['QueueConfiguration'] = {'Events': ['s3:ObjectCreated:*'],
                                      'Queue': self.SQS_QUEUE_URL
                                      }
        response = self.s3_client.put_bucket_notification(Bucket=self.S3_BUCKET_NAME,
                                                          NotificationConfiguration={
                                                              'QueueConfiguration': data['QueueConfiguration']
                                                          }
                                                          )

    def list_sqs_queues(self):
        """
        List SQS queues
        :return: None
        """
        queues = self.sqs_client.list_queues()
        print queues

    def send_sqs_messages(self, message):
        """
        Send message to SQS queue
        :param message: message to send - S3 key
        :return: None
        """
        send_response = self.sqs_client.send_message(QueueUrl=self.SQS_QUEUE_URL,
                                                     MessageBody=message)

    def poll_sqs(self):
        """
        Receive messages from SQS, create manifest from message bodies containing S3 keys, upload manifest, copy from
        manifest to redshift staging, perform merge with target table and delete SQS messages which are already read
        on successful upsert
        :return: None
        """
        receive_response = self.sqs_client.receive_message(QueueUrl=self.SQS_QUEUE_URL,
                                                           AttributeNames=['ApproximateReceiveCount'],
                                                           MessageAttributeNames=['All'],
                                                           MaxNumberOfMessages=10,
                                                           VisibilityTimeout=1800,
                                                           WaitTimeSeconds=20
                                                           )

        print receive_response
        message_list = list()
        if receive_response.get('Messages'):
            for message in receive_response['Messages']:
                message_list.append(message)

        if len(message_list) > 0:
            manifest_string = self.generate_manifests(message_list)
            manifest_key = self.post_manifest_to_s3(manifest_string)
            full_s3_path = '%s/%s' % (self.S3_BUCKET_NAME, manifest_key)
            print 'Manifest Posted to S3: %s' % full_s3_path
            copy_command = self.generate_copy_statement(full_s3_path=full_s3_path, redshift_table=self.STAGING_TABLE)
            try:
                if self.copy_to_redshift_staging(copy_command=copy_command) or 1 == 1:
                    # Delete SQS messages containing S3 keys that have already been copied to S3
                    self.delete_sqs_messages(message_list)
                    # Upsert logic
                    upsert = Upsert(staging_table=self.STAGING_TABLE, target_table=self.TARGET_TABLE)
                    upsert.process(redshift_conn=self.redshift_conn)
            except Exception as e:
                print (e)

    @staticmethod
    def generate_manifests(message_list):
        """
        Generate manifest from SQS messages containing S3 keys as body
        :param message_list: SQS messages
        :return: manifest dict
        """
        manifest_dict = dict()
        entries_list = list()
        for message in message_list:
            entries = dict()
            entries['url'] = message['Body']
            entries_list.append(entries)

        manifest_dict['entries'] = entries_list

        return manifest_dict

    def post_manifest_to_s3(self, manifest_dict):
        """
        Posts manifest to S3
        :param manifest_dict: manifest dict
        :return: None
        """
        manifest_string = json.dumps(manifest_dict)
        ts = int(time.time())
        key_name = 'manifests/manifest_%d' % ts
        self.s3_client.put_object(Bucket=self.S3_BUCKET_NAME,
                                  Body=manifest_string,
                                  Key=key_name)
        return key_name

    def delete_sqs_messages(self, messages):
        """
        Delete messages from SQS
        :param messages: messages to delete from SQS
        :return: None
        """
        for message in messages:
            delete_response = self.sqs_client.delete_message(QueueUrl=self.SQS_QUEUE_URL,
                                                             ReceiptHandle=message['ReceiptHandle']
                                                             )

    def generate_copy_statement(self, full_s3_path, redshift_table):
        copy_command = """
        COPY {redshift_table} FROM s3://{full_s3_path} access_key_id '{access_key}'
        secret_access_key '{secret_key}' manifest JSON 'auto' gzip maxerror as 100 compupdate off ACCEPTINVCHARS
        """.format(redshift_table=redshift_table,
                   full_s3_path=full_s3_path,
                   access_key=self.AWS_ACCESS_KEY,
                   secret_key=self.AWS_SECRET_KEY)

        return copy_command

    def copy_to_redshift_staging(self, copy_command):
        redshift_cursor = self.redshift_conn.cursor()
        try:
            print 'Executing: %s' % copy_command
            redshift_cursor.execute(copy_command)
            return True
        except psycopg2.DatabaseError as e:
            print(e)
            pass

        return False

    def process(self):
        while True:
            time.sleep(20)
            self.poll_sqs()

if __name__ == '__main__':
    obj = Worker()
    obj.process()


