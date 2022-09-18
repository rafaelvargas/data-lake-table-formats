

import datetime
import os
import pandas as pd
import numpy as np
import datasets
import random

SIZE = 0.01
NUMBER_OF_RECORDS = int(15785056 * SIZE)

def generate_daily_active_users():
    number_of_days = (datasets.END_DATE - datasets.START_DATE).days + 1
    daily_active_users = []
    for i in range(number_of_days - 1):
        base_active_users = int(NUMBER_OF_RECORDS / number_of_days)
        daily_active_users.append(
            base_active_users + random.randint(-int((base_active_users / 2) * 0.05), int((base_active_users / 2) * 0.05))
        )
    daily_active_users.append(NUMBER_OF_RECORDS - sum(daily_active_users))
    return daily_active_users


def generate_load_dataset(table='fact_daily_usage_by_user', destination_folder='data'):
    print(f'Generating {table}...')
    if table == 'fact_daily_usage_by_user':
        for d, number_of_active_users in zip(pd.date_range(datasets.START_DATE, datasets.END_DATE), generate_daily_active_users()):
            df = pd.DataFrame()
            date = d.date()
            print(f"Generating data for {date}...")
            for column, column_definition in datasets.tables[table].items():
                # print(f"\t [{date}] Generating values for {column}...")
                if column == "date":
                    base_date_array = np.full((1, number_of_active_users), np.datetime64(date))[0].astype(datetime.date)
                    df[column] = base_date_array
                elif column == "user_id":
                    df[column] = np.random.choice(number_of_active_users + 10000, size=number_of_active_users, replace=False)
                else:
                    df[column] = np.random.randint(column_definition['range'][0], column_definition['range'][1], number_of_active_users)
            # print(len(df.groupby(['date', 'user_id', 'plan_id', 'software_version_id', 'platform_id', 'country_id']).sum().reset_index()), number_of_active_users)
            df.to_parquet(f'{destination_folder}/load_{table}', compression='snappy', index=False, partition_cols=['date'], engine="pyarrow")
    else:
        df = pd.DataFrame.from_dict(datasets.tables[table])
        df.to_parquet(f'{destination_folder}/load_{table}.parquet', compression='snappy', index=False)


def generate_update_datasets(table='fact_daily_usage_by_user', fractions_to_generate=[.08,.16,.32], destination_folder='data'):
    for fraction in fractions_to_generate:
        print(f"Modifing the measures of {int(fraction * 100)}% of the fact table")
        for d in pd.date_range(datasets.START_DATE, datasets.END_DATE):
            date = d.date()
            data = pd.read_parquet(f'{destination_folder}/load_{table}/date={date}')
            data.insert(0, 'date', date)
            sampled_data = data.sample(frac=fraction)
            for column in ['duration_in_seconds', 'number_of_logins', 'number_of_songs_played']:
                sampled_data[column] = np.random.randint(datasets.tables[table][column]['range'][0], datasets.tables[table][column]['range'][1], size=len(sampled_data))
            sampled_data.to_parquet(f'{destination_folder}/update_{int(fraction * 100)}_{table}', compression='snappy', index=False, partition_cols=['date'])


if __name__ == '__main__':
    print(f"Generating ~{SIZE}GB dataset...")
    folder = f'data/{SIZE}gb'
    is_folder_existent = os.path.exists(folder)
    if not is_folder_existent:
        os.makedirs(folder)
    for table_name in datasets.tables.keys():
        generate_load_dataset(table_name, destination_folder=folder)
    generate_update_datasets(destination_folder=folder)
    # generate_daily_active_users()