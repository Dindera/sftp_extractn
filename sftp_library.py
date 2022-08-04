import logging
import os
import pysftp
import logging
import boto3
import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read('config.cfg')

Host = config.get('default', "HOSTNAME")
Username = config.get('default', 'USERNAME')
Password = config.get('default', 'PASSWORD')


class SFTPExtract:

    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    def load(self):
        """extracts file from sftp server """
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        """ connect to the server """
        with pysftp.Connection(host=self.host,
                               username=self.username,
                               password=self.password,
                               cnopts=cnopts) as sftp:
            print("connection successfully established...")
            sftp.cwd("/duspat-sftp-files/usr_duspat")
            dir_struct = sftp.listdir_attr()

            # list files in the directory
            for attr in dir_struct:
                print(attr.filename, attr)

            sftp.get("DE_202208_100.xlsx", "./input/DE_202208_100.xlsx")
            sftp.get("DE_202208_100.xlsx", "./input/ar_weekly_dup.csv")

        return

    def store(self, file_name, bucket, Key=None):
        """ 
        This method uploads files to s3 bucket 
        file_name - path to file 
        bucket - s3 bucket to upload file
        Key - name of file to upload
        """
        self.file_name = file_name
        self.bucket = bucket
        self.key = Key
        session = boto3.Session(profile_name='duspat')
        client = session.client('s3')

        if self.key == None:
            self.key = os.path.basename(self.file_name)

        try:
            response = client.upload_file(self.file_name, self.bucket, self.key)

        except ClientError as e:
            logging.error(e)
            return False
        return True


if __name__ == "__main__":
    sftpextract = SFTPExtract(Host, Username, Password)
    sftpextract.load()
    sftpextract.store('input/ar_weekly_dup.csv', 'sftp-etl', 'ar_weekly_dup.csv')
    sftpextract.store('input/DE_202208_100.xlsx', 'sftp-etl', 'DE_202208_100.xlsx')