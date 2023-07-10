import pytest
import gcsfs

class TestConnectivity:
    fs_gcs = gcsfs.GCSFileSystem(project='prediswiss')
    bucket_name = "prediswiss-parquet-data-daily"
    bucket_data = "prediswiss-parquet-data"

    def test_bucket_data_exist(self):
        assert self.fs_gcs.exists(self.bucket_data)
        assert self.fs_gcs.exists(self.bucket_name)