# coding=utf-8
import json
import os


def export_file(file_path):
	"""
		Efetua leitura de um arquivo .json e coloca nas variáveis de ambiente.
	:param file_path: Caminho do arquivo
	:return:
	"""
	with open(file_path) as f:
		for key, value in json.load(f).items():
			os.environ[key.upper()] = value
