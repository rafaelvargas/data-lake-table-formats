

import pandas as pd
import numpy as np
import datasets

NUMBER_OF_RECORDS = 10 ** 6

def generate_load_dataset(table='fact_daily_usage_by_user', destination_folder='data'):
    df = None

    print(f'Generating {table}')
    if table == 'fact_daily_usage_by_user':
        records = []
        counter = 0
        for i in range(NUMBER_OF_RECORDS):
            if i % int(NUMBER_OF_RECORDS / 10) == 0:
                counter += int(NUMBER_OF_RECORDS / 10)
                print(f"Generated {counter} rows.")
            records.append({
                key: value['generator'](value['range'][0], value['range'][1]) for key, value in datasets.tables[table].items()
            })
        df = pd.DataFrame.from_records(records)
        # print(df.groupby(['date', 'user_id', 'plan_id', 'software_version_id', 'platform_id', 'country_id']).sum().reset_index())
    else:
        df = pd.DataFrame.from_dict(datasets.tables[table])
    df.to_parquet(f'{destination_folder}/load_{table}.snappy.parquet', compression='snappy', index=False)

def generate_update_datasets(table='fact_daily_usage_by_user', fractions_to_generate=[.08,.16,.32], destination_folder='data'):
    data = pd.read_parquet(f'{destination_folder}/load_{table}.snappy.parquet')
    for fraction in fractions_to_generate:
        print(f"Modifing the measures of {int(fraction * 100)}% of the dataset")
        sampled_data = data.sample(frac=fraction)
        for column in ['duration_in_seconds', 'number_of_logins', 'number_of_songs_played']:
            sampled_data[column] = np.random.randint(datasets.tables[table][column]['range'][0], datasets.tables[table][column]['range'][1], size=len(sampled_data))
        sampled_data.to_parquet(f'{destination_folder}/update_{int(fraction * 100)}_{table}.snappy.parquet', compression='snappy', index=False)


if __name__ == '__main__':
    for table_name in datasets.tables.keys():
        generate_load_dataset(table_name)
    generate_update_datasets()