

import datetime
import os
import pandas as pd
import numpy as np
import datasets
from datasets import SIZE, DAILY_ACTIVE_USERS


def generate_load_dataset(table='fact_daily_usage_by_user', destination_folder='data'):
    print(f'Generating {table}...')
    if table == 'fact_daily_usage_by_user':
        for d, number_of_active_users in zip(pd.date_range(datasets.START_DATE, datasets.END_DATE), DAILY_ACTIVE_USERS):
            df = pd.DataFrame()
            date = d.date()
            print(f"Generating data for {date}...")
            for column, column_definition in datasets.tables[table].items():
                # print(f"\t [{date}] Generating values for {column}...")
                if column == "date":
                    base_date_array = np.full((1, number_of_active_users), np.datetime64(date))[0].astype(datetime.date)
                    df[column] = base_date_array
                elif column == "user_id":
                    df[column] = np.random.choice(datasets.tables['dim_user']['id'], size=number_of_active_users, replace=False)
                else:
                    df[column] = np.random.randint(column_definition['range'][0], column_definition['range'][1] + 1, number_of_active_users)
            df.to_parquet(f'{destination_folder}/load_{table}', compression='snappy', index=False, partition_cols=['date'], engine="pyarrow")
    else:
        df = pd.DataFrame.from_dict(datasets.tables[table])
        folder = destination_folder + f"/load_{table}"
        is_folder_existent = os.path.exists(destination_folder + f"/load_{table}")
        if not is_folder_existent:
            os.makedirs(folder)
        df.to_parquet(f'{destination_folder}/load_{table}/part-00001.parquet', compression='snappy', index=False)


def generate_update_datasets(table='fact_daily_usage_by_user', fractions_to_generate=[.08,.16,.32], destination_folder='data'):
    for fraction in fractions_to_generate:
        print(f"Modifing the measures of {int(fraction * 100)}% of the fact table")
        for d in pd.date_range(datasets.START_DATE, datasets.END_DATE):
            date = d.date()
            data = pd.read_parquet(f'{destination_folder}/load_{table}/date={date}')
            data.insert(0, 'date', date)
            sampled_data = data.sample(frac=fraction)
            for column in ['duration_in_seconds', 'number_of_sessions', 'number_of_songs_played']:
                sampled_data[column] = np.random.randint(datasets.tables[table][column]['range'][0], datasets.tables[table][column]['range'][1] + 1, size=len(sampled_data))
            sampled_data.to_parquet(f'{destination_folder}/update_{int(fraction * 100)}_{table}', compression='snappy', index=False, partition_cols=['date'])


if __name__ == '__main__':
    print(f"Generating ~{SIZE}GB dataset...")
    folder = f'data/{SIZE}gb'.replace('.', '_')
    is_folder_existent = os.path.exists(folder)
    if not is_folder_existent:
        os.makedirs(folder)
    for table_name in datasets.tables.keys():
        generate_load_dataset(table_name, destination_folder=folder)
    generate_update_datasets(destination_folder=folder)