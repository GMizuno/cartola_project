import json
from io import BytesIO

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Configurações
storage_account_name = "cartolaraw"
file_system_name = "team"  # Nome do File System
local_file_path = "README.md"
destination_path = "adasda/teste.md"

# Autenticação usando DefaultAzureCredential
credential = DefaultAzureCredential()

# Cria o cliente para o Data Lake Service
service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account_name}.dfs.core.windows.net",
    credential=credential,
)

# data = "Este é o conteúdo que quero fazer upload."
data = {
    "nome": "João",
    "idade": 30,
    "cidade": "São Paulo"
}

# Converte o objeto para JSON
data_bytes = json.dumps(data).encode("utf-8")

# Obtem o cliente do File System (container)
file_system_client = service_client.get_file_system_client(file_system_name)

# Obtem o cliente do arquivo no ADLS
file_client = file_system_client.get_file_client(destination_path)

# Faz upload do arquivo
file_client.upload_data(data_bytes, overwrite=True)

paths_container = file_system_client.get_paths(path=None, recursive=False)
paths = file_system_client.get_paths(path='adasda', recursive=False)

files_only = [path for path in paths if not path.is_directory]
files_container_only = [path for path in paths_container if not path.is_directory]

downloaded_file = file_client.download_file()
file_content = downloaded_file.readall().decode("utf-8")

from cartola_project.storage import factory_storage
import pandas as pd

# Configurações
storage_account_name = "cartolaraw"
file_system_name = "team"  # Nome do File System
destination_path = "adasda/teste_azure.json"
destination_path_2 = "adasda/teste_azure.parquet"
data = {
    "nome": "João",
    "idade": 30,
    "cidade": "São Paulo"
}
data_2 = pd.read_parquet('league_info.parquet')

azure_storage = factory_storage.get_storage('Azure')
azure = azure_storage(storage_account_name, file_system_name)

azure.upload(data, destination_path)
azure.upload(data_2, destination_path_2)
file_json = azure.download(destination_path)
file_parquet = azure.download(destination_path_2)
