import json

# from bagre.api import DataLake
import configparser as cfg
from lib.google_big_query_reader import GoogleBigQueryReader
from lib.google_storage_reader import GoogleStorageReader


class DataLakeGoogleTransfer:
	def __init__(self):
		self.aws_config = None
		self.get_properties()
		self.output_dir = None

		config = cfg.RawConfigParser()
		config.read("config/properties.ini")
		self.output_dir = config.get("general", "output_dir")

	# self.data_lake = None
	# self.pack_data = {}
	# self.pack_data = {
	# 	'nome': 'Teste-RodrigoGil',
	# 	'descricao': 'Google Storage To Data Lake Test',
	# 	'usuario': 'rodrigo_gil_zapcorp_com_br',
	# 	'area': "3",
	# 	'grupos': [{'id': 'cbe519ae-e8a2-4867-8234-5d3279c96c8c', 'nome': 'TI'},
	# 	           {'id': '8e95d5c5-997f-4483-93bf-7e8c04afed33', 'nome': 'Arquitetura de TI'}],
	# 	'metadata': [{'tag': 'Contato Técnico', 'valor': 'Paixao'},
	# 	             {'tag': 'Origem dos Dados', 'valor': 'Xiss'},
	# 	             {'tag': 'Information Steward', 'valor': 'Xiss'}]
	#
	# }

	def get_properties(self):
		"""
			Recupera as configurações do AWS
		"""
		with open('auth/aws.json') as f:
			self.aws_config = json.load(f)

	def transfer_all_to_storage(self, params):
		bigquery = GoogleBigQueryReader()
		bigquery.auth()
		bigquery.all_queries_to_storage(params)

	# def create_data_lake_package(self):
	# 	self.data_lake = DataLake(**self.aws_config)
	# 	self.pack_data["nome"] =

	def transfer_all_to_aws(self, bucket_name, directory):
		# data_lake = DataLake(**self.aws_config)
		google_storage = GoogleStorageReader()
		google_storage.auth()
		google_storage.set_bucket(bucket_name)
		google_storage.auth()
		blob_list = google_storage.get_blob_list(directory)
		for blob in blob_list:
			google_storage.download(blob.name)
			# TODO upload to DataLake

	def run(self, params, bucket_name):
		self.transfer_all_to_storage(params)
		self.transfer_all_to_aws(bucket_name)
