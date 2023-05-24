import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import pyarrow as pa
from datetime import datetime, timedelta
from dateutil import rrule
import xml.etree.ElementTree as ET
from google.cloud import storage
import functions_framework
import gcsfs

bucket_name = "prediswiss-parquet-data-daily"
bucket_row = "prediswiss-raw-data"

@functions_framework.cloud_event
def to_parquet_daily(cloud_event):
    storage_client = storage.Client(project="prediswiss")

    try:
        bucketRaw = storage_client.get_bucket(bucket_row)
    except:
        raise RawDataException
    
    now = datetime.now() - timedelta(minutes=3)
    dayEarlier = now - timedelta(days=1) + timedelta(minutes=1)
    
    dataframe = pd.DataFrame()

    for dt in rrule.rrule(rrule.MINUTELY, dtstart=dayEarlier, until=now):
        blob = bucketRaw.get_blob(dt.strftime("%Y-%m-%d/%H-%M"))

        if blob.exists(storage_client) == True:
            data = blob.download_as_text()

            namespaces = {
                'ns0': 'http://schemas.xmlsoap.org/soap/envelope/',
                'ns1': 'http://datex2.eu/schema/2/2_0',
            }

            dom = ET.fromstring(data)

            publicationDate = dom.find(
                './ns0:Body'
                '/ns1:d2LogicalModel'
                '/ns1:payloadPublication'
                '/ns1:publicationTime',
                namespaces
            ).text

            compteurs = dom.findall(
                './ns0:Body'
                '/ns1:d2LogicalModel'
                '/ns1:payloadPublication'
                '/ns1:siteMeasurements',
                namespaces
            )

            flowTmp = [(compteur.find(
                './ns1:measuredValue[@index="1"]'
                '/ns1:measuredValue'
                '/ns1:basicData'
                '/ns1:vehicleFlow'
                '/ns1:vehicleFlowRate',
                namespaces
            ), compteur.find(
                './ns1:measuredValue[@index="11"]'
                '/ns1:measuredValue'
                '/ns1:basicData'
                '/ns1:vehicleFlow'
                '/ns1:vehicleFlowRate',
                namespaces
            ), compteur.find(
                './ns1:measuredValue[@index="21"]'
                '/ns1:measuredValue'
                '/ns1:basicData'
                '/ns1:vehicleFlow'
                '/ns1:vehicleFlowRate',
                namespaces
            )) for compteur in compteurs]

            speedTmp = [(compteur.find(
                './ns1:measuredValue[@index="2"]'
                '/ns1:measuredValue'
                '/ns1:basicData'
                '/ns1:averageVehicleSpeed'
                '/ns1:speed',
                namespaces
            ), compteur.find(
                './ns1:measuredValue[@index="12"]'
                '/ns1:measuredValue'
                '/ns1:basicData'
                '/ns1:averageVehicleSpeed'
                '/ns1:speed',
                namespaces
            ), compteur.find(
                './ns1:measuredValue[@index="22"]'
                '/ns1:measuredValue'
                '/ns1:basicData'
                '/ns1:averageVehicleSpeed'
                '/ns1:speed',
                namespaces
            )) for compteur in compteurs]

            speed = []
            for sp in speedTmp:
                speed.append((None if sp[0] == None or sp[0] == 0 else sp[0].text, None if sp[1] == None or sp[1] == 0 else sp[1].text, None if sp[2] == None or sp[2] == 0 else sp[2].text))

            flow = []
            for fl in flowTmp:
                flow.append((None if fl[0] == None else fl[0].text, None if fl[1] == None else fl[1].text, None if fl[2] == None else fl[2].text))

            compteursId = [comp.find('ns1:measurementSiteReference', namespaces).get("id") for comp in compteurs]

            columns = ["publication_date", "id", "flow_1", "flow_11", "flow_21", "speed_2", "speed_12", "speed_22"]

            data = [(publicationDate, compteursId[i], flow[i][0], flow[i][1], flow[i][2], speed[i][0], speed[i][1], speed[i][2]) for i in range(len(compteursId)) ]
            dataframe = pd.concat([pd.DataFrame(data=data, columns=columns), dataframe])
        
    table = pa.Table.from_pandas(dataframe)
    datasetPath = dt.strftime("%Y")
    fs_gcs = gcsfs.GCSFileSystem()
    path = "gs://" + bucket_name + "/" + datasetPath + ".parquet"
    pq.write_to_dataset(table, root_path=path, filesystem=fs_gcs)

#if __name__ == "__main__":
#    to_parquet("")

def create_bucket(name, client: storage.Client):    
    bucket = client.create_bucket(name, location="us-east1")
    print(f"Bucket {name} created")
    return bucket

def create_blob(root_bucket: storage.Bucket, destination_name, data_type, data):
    blob = root_bucket.blob(destination_name)
    generation_match_precondition = 0
    blob.upload_from_string(data, data_type, if_generation_match=generation_match_precondition)
    print("file created")

class RawDataException(Exception):
    "Raised when ubucket of raw data doesn't exist"
    pass