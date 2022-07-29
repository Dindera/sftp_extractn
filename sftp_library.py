import pysftp
import boto3
import configparser

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

    def store():
        """ store file to s3 bucket """
        pass


if __name__ == "__main__":
    sftpextract = SFTPExtract(Host, Username, Password)
    sftpextract.load()