


DEFINITIONS = {
    'fact_daily_usage_by_user': {
        'primary_key': 'date,user_id,plan_id,software_version_id,platform_id,country_id', 
        'partition_columns': 'date', 
        'column_definitions': {
            'date': 'DATE,',
            'user_id': 'INT,',
            'plan_id':'INT,',
            'software_version_id': 'INT,',
            'platform_id': 'INT,',
            'country_id': 'INT,',
            'duration_in_seconds': 'INT,',
            'number_of_sessions': 'INT,',
            'number_of_songs_played': 'INT'
        }
    },
    'dim_plan': { 
        'primary_key': 'id', 
        'partition_columns': None, 
        'column_definitions': {
            'id': 'INT,',
            'name': 'STRING,',
            'price': 'FLOAT'
        }
    },
    'dim_platform': { 
        'primary_key': 'id', 
        'partition_columns': None, 
        'column_definitions': {
            'id': 'INT,',
            'name': 'STRING'
        }
    },
    'dim_software_version': { 
        'primary_key': 'id', 
        'partition_columns': None, 
        'column_definitions': {
            'id': 'INT,',
            'version': 'STRING',
        }
    },
    'dim_country': { 
        'primary_key': 'id', 
        'partition_columns': None, 
        'column_definitions': {
            'id': 'INT,',
            'name': 'STRING',
        }
    },
    'dim_user': { 
        'primary_key': 'id', 
        'partition_columns': None, 
        'column_definitions': {
            'id': 'INT,',
            'name': 'STRING,',
            'age': 'INT'
        }
    }
}
