import configparser as cfg
import logging
import uuid

from util.job import wait_for_job

from google.cloud import bigquery
from google.cloud.bigquery.job import ExtractTableToStorageJob


class BigQueryReader:
    """
        Efetua leitura e transferências de dados do Google Cloud Big Query.
    """

    def __init__(self):
        self.client = None  # type: bigquery.Client
        self.google_storage_url = None
        self.key_file = None
        self.config = None  # type: cfg.ConfigParser
        self.get_properties()

    def get_properties(self):
        """
            Recupera as configuração do arquivo de propriedades.
            :return: None
        """
        self.config = cfg.ConfigParser()
        self.config.read("config/properties.ini")
        self.google_storage_url = self.config.get("connection", "google-storage-url")
        self.key_file = self.config.get("auth", "key-file")

    def auth(self):
        """
            Faz a autenticação usando o JSON
        """
        self.client = bigquery.Client.from_service_account_json("key.json")

    def get_job_name(self):
        """
            Imprime o nome de todos os jobs existentes.
        """

        for job in self.client.list_jobs():  # type: ExtractTableToStorageJob
            print(job.name)

    def extract_table(self, dataset_name: str, table: bigquery.Table, destination_folder: str = None):
        """
            Extrai um dataset do bigquery para o storage configurado
            :param destination_folder: Nome da pasta de destino
            :param table: Tabela que será exportada
            :param dataset_name: Nome do dataset que será exportado
        """
        if destination_folder is None:
            destination_folder = table.name

        job = self.client.extract_table_to_storage(
            "extract_job_{}".format(str(uuid.uuid4())), table, self.google_storage_url + destination_folder + "/*")
        job.begin()
        wait_for_job(job)
        logging.debug('Exported {}:{} to {}'.format(dataset_name, table.name, self.google_storage_url))

    def extract_dataset(self, dataset_name: str):
        """
            Extrai um dataset do bigquery para o storage configurado
            :param dataset_name: Nome do dataset que será exportado
        """
        table_list = self.client.dataset(dataset_name).list_tables()
        logging.debug('Starting to export...')
        for table in table_list:  # type: bigquery.Table
            self.extract_table(dataset_name, table)
        logging.debug('Done.')

    def extract_query(self, destination_folder: str, query: str):
        """
            Executa uma consulta, extraindo a tabela para o diretório informado.
        :param destination_folder: Diretório de destino
        :param query: Consulta que será realizada no bigquery
        """
        query_job = self.client.run_async_query(str(uuid.uuid4()), query)
        query_job.use_legacy_sql = False
        query_job.begin()
        query_job.result()  # Espera o job da query finalizar.
        destination_table = query_job.destination  # type: bigquery.Table
        self.extract_table(dataset_name="", table=destination_table, destination_folder=destination_folder)

    def extract_all_queries(self):
        """
            Extrai todos as consultas definidas no arquivo de configuração.
        """
        queries_dict = dict(self.config.items("query"))
        for key, value in queries_dict.items():
            self.extract_query(key, value)
