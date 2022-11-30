import boto3


class AwsConnection:

    def __init__(self, access_key, secret_access):
        self.secret_access = secret_access
        self.access_key = access_key

    @property
    def client(self):
        return boto3.client(
            's3',
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_access
        )
    @property
    def storage_option(self):
        return {'key': self.access_key,'secret': self.secret_access}
