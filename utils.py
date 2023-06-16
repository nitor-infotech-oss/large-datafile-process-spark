import get_secrets
import tempfile
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient

#from get_secrets import GetSecrets
secrets = get_secrets.GetSecrets().get_secrets()


class BlobOperations:
    def __init__(self, blob_name: str, secret_dict: dict):
        self.secret_dict = secret_dict
        self.blob_service_client = BlobServiceClient.from_connection_string(
            secret_dict["connection_string"]
        )
        self.blob_client = self.blob_service_client.get_blob_client(
            container=secret_dict["container_name"], blob=blob_name
        )
        self.blob_name = blob_name
        self.spark = SparkSession.builder.appName("BlobOperations").getOrCreate()
        self.container_client = self.blob_service_client.get_container_client(secret_dict["container_name"])

    def download_blob(self):
        # Create a temporary file to store the blob data
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            blob_data = self.blob_client.download_blob().readall()
            temp_file.write(blob_data)
            self.file_path = temp_file.name

        # Read the file using PySpark
        '''If uneven spaces prevented you from getting all the columns, use the following code:'''
        
        # cols = ["_cc" + str(i) for i in range(0, 40)]
        '''The range would be determined by how many columns your file has.'''
        
        # mySchema = StructType([StructField(c, StringType()) for c in cols])
        '''Provide this schema while reading spark df to get all the columns'''
       
        self.df = self.spark.read.csv(
            self.file_path, sep=r"\t", encoding="UTF-16", header=False
        )

        return self.df
    
    def upload_blob(self, file_contents, blob_report_name):
        blob_client = self.container_client.get_blob_client(blob=blob_report_name)
        blob_client.upload_blob(file_contents, overwrite=True)
        print(f"Uploaded {blob_report_name} to Azure Blob Storage.")


