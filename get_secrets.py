import logging
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

"""Provide below credentials from Azure active directory"""

os.environ["AZURE_CLIENT_ID"] = "{AZURE_CLIENT_ID}"
os.environ["AZURE_CLIENT_SECRET"] = "{Secret_Value}"
os.environ["AZURE_TENANT_ID"] = "{AZURE_TENANT_ID}"

"""
Provide the names of KeyVault as well as the secret names that record the value of the 
connection string and container name for the following class.
"""
class GetSecrets:
    def __init__(self):
        self.__secret_client = SecretClient(
            vault_url="https://{KeyVaultName}.vault.azure.net/",
            credential=DefaultAzureCredential()
        )

    def get_secrets(self):
        secret_dict = {}
        try:
            connection_string = self.__secret_client.get_secret("{connectionstring}").value
            container_name = self.__secret_client.get_secret("{containername}").value
        except Exception as e:
            logging.error("Error retrieving secrets: {}".format(str(e)))
            return None

        if connection_string is None or container_name is None:
            logging.error("Missing secrets")
            return None

        secret_dict.update({
            "connection_string": connection_string,
            "container_name": container_name
        })

        return secret_dict