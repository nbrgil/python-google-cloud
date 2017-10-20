import configparser as cfg
import logging
import uuid

from google.cloud.bigquery import job as bigquery_job
from util.job import wait_for_job
from google.cloud import bigquery


class GoogleBigQueryReader:
	"""
		Efetua leitura e transferências de dados do Google Cloud Big Query.
	"""

	def __init__(self):
		self.client = None  # type: bigquery.Client
		self.google_storage_url = None
		self.key_file = None
		self.config = None  # type: cfg.ConfigParser
		self.queries_dict = None
		self.get_properties()

	def get_properties(self):
		"""
			Recupera as configuração do arquivo de propriedades.
		"""
		config = cfg.RawConfigParser()
		config.read("config/properties.ini")
		self.google_storage_url = config.get("connection", "google-storage-url")
		self.key_file = config.get("auth", "key-file")

		config = cfg.RawConfigParser()
		config.read("config/queries.ini")
		self.queries_dict = dict(config.items())

	def auth(self):
		"""
			Faz a autenticação usando o JSON
		"""
		self.client = bigquery.Client.from_service_account_json(self.key_file)

	def table_to_storage(self, dataset_name: str, table: bigquery.Table, file_prefix: str = None):
		"""
			Extrai um dataset do bigquery para o storage configurado
			:param file_prefix: Prefixo do arquivo
			:param table: Tabela que será exportada
			:param dataset_name: Nome do dataset que será exportado
		"""
		if file_prefix is None:
			file_prefix = table.name

		job = self.client.extract_table_to_storage(
			"extract_job_{}".format(str(uuid.uuid4())), table,
			self.google_storage_url + file_prefix.upper() + "-*.gzip")
		job.compression = bigquery_job.Compression.GZIP
		job.begin()
		wait_for_job(job)
		logging.debug('Exported {}:{} to {}'.format(dataset_name, table.name, self.google_storage_url))

	def dataset_tables_to_storage(self, dataset_name: str):
		"""
			Extrai as tabelas de um dataset do bigquery para o storage configurado
			:param dataset_name: Nome do dataset que será exportado
		"""
		table_list = self.client.dataset(dataset_name).list_tables()
		logging.debug('Starting to export...')
		for table in table_list:  # type: bigquery.Table
			self.table_to_storage(dataset_name, table)
		logging.debug('Done.')

	def query_to_storage(self, query_key: str, query_params: {} = None):
		"""
			Executa uma consulta, extraindo a tabela para o diretório informado.
			:param query_key: Chave da sessão no arquivo de consultas
			:param query_params: Dicionário com todos os parâmetros da query
		"""
		query_section = self.queries_dict[query_key.lower()]
		query_job = self.client.run_async_query(
			str(uuid.uuid4()),
			query_section["query"].format(**query_params),
		)
		query_job.use_legacy_sql = bool(query_section["legacy"])
		query_job.begin()
		logging.debug("Executing query: " + str(query_section))
		query_job.result()  # Espera o job da query finalizar.
		destination_table = query_job.destination  # type: bigquery.Table
		logging.debug("Exporting table: " + str(query_section))
		self.table_to_storage(dataset_name="", table=destination_table, file_prefix=query_section["prefix"])
		logging.debug("Export Ok!")

	def run_sync_query(self, query: str, max_results: int, legacy_sql: bool = True):
		"""
			Executa uma query qualquer, retornando um cursor.
			:param legacy_sql: Define se a consulta que vai ser executada está ou não como legacy no Google Cloud
			:param query: Consulta que será executada
			:param max_results: Qtde máxima de resultados
		"""
		query_results = self.client.run_sync_query(query)
		query_results.use_legacy_sql = legacy_sql
		query_results.run()

		fetch_result = query_results.fetch_data(
			max_results=max_results
		)
		return fetch_result

	def all_queries_to_storage(self, query_params: {} = None):
		"""
			Extrai todos as consultas definidas no arquivo de configuração.
			:type query_params: Dicionário com todos os parâmetros da query
				Inicialmente somente com dois :
					start_date: Filtro de data inicial
					end_date: Filtro de data final
		"""

		for key, value in self.queries_dict.items():
			if key != "DEFAULT":
				self.query_to_storage(key, query_params)
