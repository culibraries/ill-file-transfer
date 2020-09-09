#!/usr/bin/python3

import boto3
import os, subprocess

class ILLBucket:
    """ Represents the ILL file transfer bucket """

    def __init__(self, bucket):
        self._s3 = boto3.resource('s3')
        self._bucket = self._s3.Bucket(bucket)
        self._bucket_name = bucket
        self._doc_list = self._get_doc_list()

    def _get_doc_list(self):
        """ Gets a list of objects from the bucket that need processing """
        doc_list = list()
        for doc in self._bucket.objects.all():
            if doc.key.endswith('pdf'):
                doc_list.append(doc.key)
        return doc_list

    def doc_list(self):
        """ Returns a list of documents for transfer """
        return self._doc_list

    def download(self, doc):
        """ Download a document from S3 to EC2 """
        with open(doc, 'wb') as data:
            self._bucket.download_fileobj(doc, data)
    
    def mark_as_processed(self, doc):
        """ After the document has been transferred, mark it as processed """
        copy_source = {'Bucket':self._bucket_name, 'Key':doc}
        self._bucket.copy(copy_source, '{}.processed'.format(doc))
        self._bucket.delete_objects(Delete={'Objects':[{'Key':doc}], 'Quiet':True})

if __name__ == "__main__":

    # If there are documents to be transferred, they will be done here in sequence.
    #
    # The former FTP method has been replaced with secure (SSH) file transfer, which
    # is run as a subprocess using sshpass. Arguments:
    #
    # -e         use SSHPASS environment variable for password
    # scp        command to run (in this case, secure copy)
    # -P 222     port number on the remote host (part of scp)
    # doc        name of the document to be transferred (part of scp)
    # user@host  user and host names (part of scp)
    #
    # The remaining subprocess parameters are self-explanatory or check the Python documentation.
    #
    # If the file transfer process fails for any reason, an alert will be published to an AWS SNS
    # topic and the process will abend.

    bucket = ILLBucket('cubl-ill')
    if len(bucket.doc_list()) != 0:
        os.chdir('/home/ec2-user/scripts')
        try:
            for doc in bucket.doc_list():
                bucket.download(doc)
                cp = subprocess.run(
                    args=['sshpass',
                          '-e',
                          'scp',
                          '-P 222',
                          doc,
                          'hosted@206.107.44.246:PDF/'],
                    stdout=subprocess.DEVNULL,
                    shell=True,
                    timeout=60,
                    check=True)
                cp.check_returncode()
                bucket.mark_as_processed(doc)
                os.remove(doc)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            sns = boto3.client('sns')
            sns.publish(
                TopicArn='arn:aws:sns:us-west-2:735677975035:ILLFileTransfer',
                Message='The ILL file transfer process has stopped.',
                Subject='ILL File Transfer Alert')
        finally:
            exit()
