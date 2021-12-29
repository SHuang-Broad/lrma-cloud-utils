import re
from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """
    Copied from
    https://cloud.google.com/storage/docs/uploading-objects#uploading-an-object
    Uploads a file to the bucket.
    :param bucket_name: your-bucket-name
    :param source_file_name: "local/path/to/file"
    :param destination_blob_name: "storage-object-name"
    :return:
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        "File {} uploaded to {}.".format(
            source_file_name, destination_blob_name
        )
    )


class GcsPath:
    """
    Modeling after GCS storage object, offering simplistic way of
        * checking if the paths exists, and if exists,
        * represents a file or
        * emulates a 'directory'.
    """

    def __init__(self, gs_path: str):

        if not gs_path.startswith("gs://"):
            raise ValueError(f"Provided gs path isn't valid: {gs_path}")

        arr = re.sub("^gs://", '', gs_path).split('/')
        self.bucket = arr[0]
        self.prefix = '/'.join(arr[1:-1])
        self.file = arr[-1]

    def get_blob(self, client: storage.client.Client) -> storage.Blob:
        return storage.Blob(bucket=client.bucket(self.bucket), name=f'{self.prefix}/{self.file}')

    def exists(self, client: storage.client.Client) -> bool:
        return self.is_file(client=client) or self.is_emulate_dir(client=client)

    def is_file(self, client: storage.client.Client) -> bool:
        return storage.Blob(bucket=client.bucket(self.bucket), name=f'{self.prefix}/{self.file}').exists(client)

    def is_emulate_dir(self, client: storage.client.Client) -> bool:
        if self.is_file(client=client):
            return False
        return any(True for _ in client.list_blobs(client.bucket(self.bucket), prefix=f'{self.prefix}/{self.file}'))

    def size(self, client: storage.client.Client) -> int:
        blob = storage.Blob(bucket=client.bucket(self.bucket), name=f'{self.prefix}/{self.file}')
        if blob.exists(client=client):
            blob.reload()
            return blob.size
        else:
            return 0