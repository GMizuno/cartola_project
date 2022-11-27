from datetime import date
import dask.dataframe as dd
from utils.util import win_home, win


def get_all_ids(AcessKey, SecretKey) -> list:
    d = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet',
                        storage_options={
                                'key': AcessKey,
                                'secret': SecretKey
                        }
                        )
    return list(set(d.id_team_home.drop_duplicates().compute().to_list() + \
                    d.id_team_away.drop_duplicates().compute().to_list()
                    )
                )


def filter_by_date(AcessKey, SecretKey, date_from: date, date_to: date):
    dask_dataframe = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet',
                                     storage_options={
                                             'key': AcessKey,
                                             'secret': SecretKey
                                     }
                                     )
    dask_dataframe["reference_date"] = dd.to_datetime(dask_dataframe["reference_date"], format='%d-%m-%Y')
    return dask_dataframe.query('reference_date >= @date_from and reference_date <= @date_to',
                                  local_dict={"date_from": date_from,
                                              "date_to": date_to}
                                  ). \
        match_id. \
        compute(). \
        to_list()


def create_obt(AcessKey, SecretKey) -> None:
    dask_dataframe1 = dd.read_parquet('s3://bootcamp-silver/matches/*.parquet',
                                      storage_options={
                                              'key': AcessKey,
                                              'secret': SecretKey
                                      }
                                      )

    dask_dataframe2 = dd.read_parquet('s3://bootcamp-silver/statistics/*.parquet',
                                      storage_options={
                                              'key': AcessKey,
                                              'secret': SecretKey
                                      }
                                      )

    dask_dataframe3 = dd.read_parquet('s3://bootcamp-silver/teams/*.parquet',
                                      storage_options={
                                              'key': AcessKey,
                                              'secret': SecretKey
                                      }
                                      )
    dask_dataframe3 = dask_dataframe3.astype({'team_id': 'int64'})

    result = dask_dataframe1.merge(dask_dataframe2, how="inner", on=['match_id'])
    result = result.merge(dask_dataframe3, how="inner", on=['team_id'])

    result = result.assign(home=result.id_team_home == result.team_id)
    result['win_home'] = result.apply(win_home, axis=1, meta='object')
    result['win'] = result.apply(win, axis=1, meta='object')
    result = result.drop(columns='win_home')

    return result.drop_duplicates().compute()
