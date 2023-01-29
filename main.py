from cartola_project import JSONReader, ParquetReader, GCSStorage, JsonWriter, ParquetWriter
from get_data.process import get_all_ids

gcs = GCSStorage('cartola.json', 'cartola-360814')
json_reader = JSONReader(gcs, 'teste_cartola_gabriel', 'teste_json.json').read()
parquet_reader = ParquetReader(gcs, 'teste_cartola_gabriel', 'obt.parquet').read()
json_reader_list = [json_reader, json_reader]

JsonWriter(gcs, 'teste_cartola_gabriel', 'teste_json_writer.json', json_reader).write()
JsonWriter(gcs, 'teste_cartola_gabriel', 'teste_json_writer_list.json', json_reader_list).write()
ParquetWriter(gcs, 'teste_cartola_gabriel', 'obt_writer.parquet', parquet_reader).write()

gcs = GCSStorage('cartola.json', 'cartola-360814')
json_reader = JSONReader(gcs, 'teste_cartola_gabriel', 'matches/bronze/league=1/season=2022/').read_all_files()
ParquetReader(gcs, 'teste_cartola_gabriel', 'matches/silver/').read_all_files()

get_all_ids(gcs, 1, 2022)
