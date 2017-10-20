import configparser as cfg
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from google.cloud.storage.blob import Blob


class GoogleStorageReader:
	"""
		Efetua leitura e exporta dados do Google Cloud Storage.
	"""

	def __init__(self):
		self.client = None  # type: storage.Client
		self.google_storage_url = None
		self.key_file = None
		self.config = None  # type: cfg.ConfigParser
		self.queries_dict = None
		self.bucket = None
		self.output_dir = None
		self.get_properties()


	def get_properties(self):
		"""
			Recupera as configuração do arquivo de propriedades.
		"""
		config = cfg.RawConfigParser()
		config.read("config/properties.ini")
		self.key_file = config.get("auth", "key-file")
		self.output_dir = config.get("general", "output_dir")

		config = cfg.RawConfigParser()
		config.read("config/queries.ini")
		self.queries_dict = dict(config.items())

	def auth(self):
		"""
			Faz a autenticação usando o JSON
		"""
		self.client = storage.Client.from_service_account_json(self.key_file)

	def set_bucket(self, bucket_name: str):
		"""
			Define qual o bucket que será alvo das operações.
		:param bucket_name: Nome do bucket
		"""
		self.bucket = self.client.get_bucket(bucket_name)  # type: Bucket

	def get_blob_list(self, prefix: str):
		"""
			Obtém a lista de arquivos existentes
		:param prefix: Prefixo do arquivo (normalmente o nome do diretório)
		:return: Lista de arquivos (tipo complexo que contém bucket name e file name)s
		"""
		blobs = self.bucket.list_blobs(prefix=prefix, delimiter="/")

		return [blob for blob in blobs if blob.name != prefix]

	def download(self, file_name: str):
		"""
			Faz o download de um arquivo do bucket definido.
		:param prefix: Prefixo do arquivo (normalmente o nome do diretório)
		:param file_name: Nome do arquivo que será baixado
		"""

		blob = self.bucket.get_blob(blob_name=file_name)  # type: Blob
		blob.download_to_filename(self.output_dir + file_name.split('/')[-1])