import pytest
import os

from google.cloud import storage
from main import create_blob, create_bucket

class TestBucket:
    bucket_name = "prediswiss_test_bucket"
    blob_name = "test"
    blob_type = "text/xml"
    blob_data = "<test>test</test>"
    storage_client = storage.Client(project="prediswiss")

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        yield
        try:
            bucket = self.storage_client.get_bucket(self.bucket_name)
            bucket.delete(force=True)
        except:
            print("ok : no bucket for test")

    def test_create_bucket(self):
        create_bucket(self.bucket_name, self.storage_client)
        try:
            self.storage_client.get_bucket(self.bucket_name)
        except Exception:
            assert False

    def test_create_blob(self):
        bucket = create_bucket(self.bucket_name, self.storage_client)
        create_blob(bucket, self.blob_name, self.blob_type, self.blob_data)
        assert bucket.get_blob(self.blob_name).download_as_text() == self.blob_data