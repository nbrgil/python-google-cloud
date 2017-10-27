# coding=utf-8
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob


class GoogleStorageWrapper:
	"""
		Efetua leitura e exporta dados do Google Cloud Storage.
	"""

	def __init__(self, key_file):
		self.__key_file = key_file
		self.__client = None

	def get_client(self):
		if self.__client is None:
			self.__client = storage.Client.from_service_account_json(self.__key_file)  # type: storage.Client

		return self.__client

	def get_bucket(self, bucket_name):
		""""""
		return self.get_client().get_bucket(bucket_name)  # type: Bucket

	def get_blob_list(self, bucket_name, dir_name, remove_current_dir=True, remove_path=True):
		"""
			Obtem a lista de arquivos existentes
		:param bucket_name: Nome do bucket
		:param dir_name: Nome do diretório (sem a barra)
		:param remove_current_dir: Indica se deve remover o diretório do prefixo
		:param remove_path: Remove o caminho do nome do arquivo
		:return:
		"""

		def __blob_split_name(blob_obj):
			blob_obj.name = blob_obj.name.split("/")[-1]
			return blob_obj

		blob_list = self.get_bucket(bucket_name).list_blobs(prefix=dir_name + "/", delimiter="/")

		if remove_current_dir:
			blob_list = [blob for blob in blob_list if blob.name != dir_name + "/"]

		if remove_path:
			blob_list = list(map(__blob_split_name, blob_list))

		return blob_list

	def download(self, bucket_name, file_name, output_file_name):
		"""
			Faz o download de um arquivo do bucket definido.
		:param bucket_name: Nome do bucket
		:param file_name: Nome completo do arquivo que será baixado
		:param output_file_name: Nome completo do arquivo de saída
		"""

		blob = self.get_bucket(bucket_name).get_blob(blob_name=file_name)  # type: Blob
		blob.download_to_filename(output_file_name)

	def delete_file(self, bucket_name, file_name):
		"""'
			Exclui um arquivo de um bucket.

			Exemplo:
				x = GoogleStorageWrapper("auth/zap.json")
				x.delete_file("zap-bi", "BI_Processo_Diario/BQ_LEADS_DIARIO_20171017-000000000000.gzip")

		:param bucket_name: Nome do bucket
		:param file_name: Nome completo do arquivo que será excluído
		"""
		blob = self.get_bucket(bucket_name=bucket_name).get_blob(blob_name=file_name)  # type: Blob
		blob.delete()

	def delete_directory_files(self, bucket_name, dir_name):
		"""
			Exclui todos os arquivos de um diretório no bucket
		:param bucket_name: Nome do bucket
		:param dir_name: Nome do diretório
		:return:
		"""
		file_list = self.get_blob_list(bucket_name, dir_name)
		for file_name in file_list:
			self.delete_file(bucket_name, file_name)
