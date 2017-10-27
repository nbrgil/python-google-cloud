import logging
import uuid

from google.cloud.bigquery import job as bigquery_job
from util.job import wait_for_job
from google.cloud import bigquery


class GoogleBigQueryWrapper:
	"""
		Facilita o uso da biblioteca do Google BigQuery.
	"""

	def __init__(self, key_file, google_storage_url):
		self.__key_file = key_file
		self.__google_storage_url = google_storage_url
		self.__client = None  # type: bigquery.Client

	@staticmethod
	def __generate_job_id(prefix):
		"""
			Metodo estatico somente para criar job ids.
		:param prefix: Prefixo do nome gerado.
		:return: Nome gerado.
		"""
		return prefix.format(str(uuid.uuid4()))

	def __get_client(self):
		"""
			Faz a autenticacao usando o JSON na primeira vez. Depois o objeto somente eh retornado.
		"""
		if self.__client is None:
			self.__client = bigquery.Client.from_service_account_json(self.__key_file)  # type: bigquery.Client

		return self.__client

	def get_table(self, dataset_name, table_name):
		"""
			Busca o objeto bigquery.Table usando o nome.
		:param dataset_name: Nome do dataset.
		:param table_name: Nome da tabela.
		:return: Objeto encontrado ou None.
		"""
		return self.__get_client().dataset(dataset_name).table(table_name)

	def get_dataset_tables(self, dataset_name):
		"""
			Busca todas as tabelas de um dataset
		:param dataset_name: Nome do dataset
		:return:
		"""
		return self.__get_client().dataset(dataset_name).list_tables()

	def table_to_storage(
			self, dataset_name, table, job_prefix="extract_job_{}",
			file_prefix=None, compression=None, file_ext=".csv"
	):
		"""
			Extrai um dataset do bigquery para o storage configurado
		:param dataset_name: Nome do dataset
		:param table: Objeto da tabela
		:param job_prefix: Prefixo do job criado
		:param file_prefix: Prefixo do arquivo criado
		:param compression: Define se tem compressao de arquivo
		:param file_ext: Extensao do arquivo gerado
		"""
		if file_prefix is None:
			file_prefix = table.name

		job_id = self.__generate_job_id(job_prefix)
		destiny_url = self.__google_storage_url + file_prefix.upper() + "-*" + file_ext

		job = self.__get_client().extract_table_to_storage(job_id, table, destiny_url)
		job.compression = compression
		job.begin()
		wait_for_job(job)
		logging.debug('Exported {}:{} to {}'.format(dataset_name, table.name, self.__google_storage_url))

	def query_to_storage(
			self, query, legacy_sql,
			query_params=None, job_prefix="extract_job_{}",
			file_prefix=None, compression=None, file_ext=".csv"
	):
		"""
			Executa uma consulta, extraindo a tabela para o diretorio informado.
		:param query: Consulta a executar
		:param legacy_sql: Informa se a consulta eh Legacy ou Standard
		:param query_params: Parametros das consultas
		:param job_prefix: Prefixo do job criado
		:param file_prefix: Prefixo do arquivo criado
		:param compression: Define se tem compressao de arquivo
		:param file_ext: Extensao do arquivo gerado
		"""

		job_id = self.__generate_job_id(job_prefix)
		query_job = self.__get_client().run_async_query(
			job_id,
			query.format(**query_params),
		)

		query_job.use_legacy_sql = legacy_sql
		query_job.begin()
		query_job.result()  # Espera o job da query finalizar.

		self.table_to_storage(
			"",
			query_job.destination,
			job_prefix,
			file_prefix,
			compression,
			file_ext
		)
