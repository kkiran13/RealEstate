from worker import Worker
from upsert import Upsert
import os


class SimpleETL(Worker):
    """
    Simple class to upload CSV file to S3, write message to SQS with S3 keu (similar to what bucket notification does)
    download S3 file to local, insert to staging table from csv, upsert from staging to target table
    and delete SQS message
    """
    def __init__(self):
        super(SimpleETL, self).__init__()
        self.staging_table = 'staging.transactions'
        self.target_table = 'public.transactions'

    def upload_file_to_s3(self):
        """
        Upload file from local to S3
        :return: None
        """
        self.s3_client.upload_file('/opt/sample.csv', self.S3_BUCKET_NAME, 'sample.csv')
        m = 's3://%s/sample.csv' % self.S3_BUCKET_NAME
        self.send_sqs_messages(m)

    def poll_sqs(self):
        """
        Poll SQS to read messages from it, parse message body to get name of CSV file, download to local,
        insert to staging and call upsert to merge staging and target tables
        :return: None
        """
        receive_response = self.sqs_client.receive_message(QueueUrl=self.SQS_QUEUE_URL,
                                                           AttributeNames=['ApproximateReceiveCount'],
                                                           MessageAttributeNames=['All'],
                                                           MaxNumberOfMessages=10,
                                                           VisibilityTimeout=1800,
                                                           WaitTimeSeconds=20
                                                           )

        message_list = list()
        if receive_response.get('Messages'):
            for message in receive_response['Messages']:
                message_list.append(message)

        for message in message_list:
            m = message['Body']
            m = m[m.rfind('/')+1:]
            self.download_s3_file(m)
            self.insert_into_staging_table(m)
            os.remove(m)
            if self.call_upsert():
                self.delete_sqs_message(message)

    def download_s3_file(self, s3_file):
        """
        Download csv file from S3 to local
        :param s3_file: file on S3
        :return: None
        """
        self.s3_client.download_file(self.S3_BUCKET_NAME, s3_file, s3_file)

    def delete_sqs_message(self, message):
        """
        Delete SQS message after S3 file specified by message body is processed
        :param message: SQS message containing S3 key
        :return: None
        """
        delete_response = self.sqs_client.delete_message(QueueUrl=self.SQS_QUEUE_URL,
                                                         ReceiptHandle=message['ReceiptHandle']
                                                         )

    def insert_into_staging_table(self, csvfile):
        """
        Insert from CSV to staging table
        :param csvfile: csv file containing records
        :return: None
        """
        import csv
        cur = self.redshift_conn.cursor()
        qry = "INSERT INTO {tablename} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)".format(tablename=self.staging_table)
        with open(csvfile, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                cur.execute(qry, row)
        self.redshift_conn.commit()

    def call_upsert(self):
        try:
            Upsert(staging_table=self.staging_table, target_table=self.target_table).process(self.redshift_conn)
            return True
        except Exception as e:
            return False

# if __name__ == '__main__':
#     obj = SimpleETL()
#     obj.upload_file_to_s3()   # upload_file_to_s3() method call can be removed when bucket notification to send events to SQS work on real AWS account
#     obj.poll_sqs()



