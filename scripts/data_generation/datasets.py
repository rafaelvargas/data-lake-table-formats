
from faker import Faker
import datetime
import random
import numpy as np

RANDOM_SEED = 42

fake = Faker()
Faker.seed(RANDOM_SEED)

random.seed(RANDOM_SEED)

START_DATE = datetime.date(2022, 8, 1)
END_DATE = datetime.date(2022, 8, 31)

NUMBER_OF_COUNTRIES = 4
NUMBER_OF_VERSIONS = 10
NUMBER_OF_USERS = 10000

tables = {
    'fact_daily_usage_by_user': {
        'date': {
            'generator': fake.date_between,
            'range': (START_DATE, END_DATE)
        },
        'user_id': {
            'generator': random.randint,
            'range': (1, 100000)
        },
        'plan_id': {
            'generator': random.randint,
            'range': (1, 10)
        },
        'software_version_id': {
            'generator': random.randint,
            'range': (1, 10)
        },
        'platform_id': {
            'generator': random.randint,
            'range': (1, 10)
        },
        'country_id': {
            'generator': random.randint,
            'range': (1, 100)
        },
        'duration_in_seconds': {
            'generator': random.randint,
            'range': (1, 86400)
        },
        'number_of_logins': {
            'generator': random.randint,
            'range': (1, 40)
        },
        'number_of_songs_played': {
            'generator': random.randint,
            'range': (1, 50)
        }
    },
    'dim_plan': {
        'id': [1,2,3,4],
        'name': ['free', 'premium', 'family', 'gold'],
        'price': [0.0, 19.99, 39.99, 49.99]
    },
    'dim_platform': {
        'id': [1,2,3],
        'name': ['desktop', 'mobile', 'web']
    },
    'dim_software_version': {
        'id': [i for i in range(1, 11)],
        'version': [f"1.{i}" for i in range(1, NUMBER_OF_VERSIONS + 1)]
    },
    'dim_country': {
        'id': [i for i in range(1, NUMBER_OF_COUNTRIES + 1)],
        'name': ['Brazil', 'United States', 'Germany', 'France']
    },
    'dim_user': {
        'id': [i for i in range(1, NUMBER_OF_USERS + 1)],
        'name': [fake.name() for _ in range(1, NUMBER_OF_USERS + 1)],
        'age': np.random.randint(14, 90, size=NUMBER_OF_USERS)
    }
}

# np.random.randint(14, 90, size=NUMBER_OF_USERS)