from lib.big_query_reader import BigQueryReader

x = BigQueryReader()
x.auth()
#x.extract_dataset("TESTE_DATA")
# x.extract_query("trip")
x.extract_all_queries()