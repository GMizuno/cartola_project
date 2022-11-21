from datetime import date

from decouple import config
import dask.dataframe as dd


def get_all_ids() -> list:
    d = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet',
                        storage_options={
                            'key'   :config('AcessKey'),
                            'secret':config('SecretKey')
                        })
    return list(set(d.id_team_home.drop_duplicates().compute().to_list() + \
                    d.id_team_away.drop_duplicates().compute().to_list()))


def filter_by_date():
    d = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet',
                        storage_options={
                            'key'   :config('AcessKey'),
                            'secret':config('SecretKey')
                        })

    date_from = date(2022, 10, 1)
    date_to = date(2022, 11, 12)
    d["reference_date"] = dd.to_datetime(d["reference_date"], format='%d-%m-%Y')
    result = d.query('reference_date >= @date_from and reference_date <= @date_to',
                     local_dict={"date_from":date_from,
                                 "date_to"  :date_to}). \
        match_id. \
        compute(). \
        to_list()


d1 = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet',
                     storage_options={
                         'key'   :config('AcessKey'),
                         'secret':config('SecretKey')
                     })

d2 = dd.read_parquet('s3://bootcamp-silver/statistics/*.parquet',
                     storage_options={
                         'key'   :config('AcessKey'),
                         'secret':config('SecretKey')
                     })

d3 = dd.read_parquet('s3://bootcamp-silver/teams/*.parquet',
                     storage_options={
                         'key'   :config('AcessKey'),
                         'secret':config('SecretKey')
                     })
d3 = d3.astype({'team_id': 'int64'})

result = d1.merge(d2, how="inner", on=['match_id'])
result = result.merge(d3, how="inner", on=['team_id'])
result = result.assign(home=result.id_team_home == result.team_id)