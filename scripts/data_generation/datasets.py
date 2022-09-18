
import datetime
import random

import numpy as np
from mimesis import Person

RANDOM_SEED = 42

person = Person(seed=RANDOM_SEED)

random.seed(RANDOM_SEED)

START_DATE = datetime.date(2022, 8, 1)
END_DATE = datetime.date(2022, 8, 31)

NUMBER_OF_COUNTRIES = 4
NUMBER_OF_VERSIONS = 10

SIZE = 2
NUMBER_OF_RECORDS = int(15785056 * SIZE)


def generate_daily_active_users():
    number_of_days = (END_DATE - START_DATE).days + 1
    daily_active_users = []
    for i in range(number_of_days - 1):
        base_active_users = int(NUMBER_OF_RECORDS / number_of_days)
        daily_active_users.append(
            base_active_users + random.randint(-int((base_active_users / 2) * 0.05), int((base_active_users / 2) * 0.05))
        )
    daily_active_users.append(NUMBER_OF_RECORDS - sum(daily_active_users))
    return daily_active_users

DAILY_ACTIVE_USERS = generate_daily_active_users()
NUMBER_OF_USERS = max(DAILY_ACTIVE_USERS) * 4


tables = {
    'fact_daily_usage_by_user': {
        'date': {
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
        'id': np.arange(1, NUMBER_OF_USERS + 1),
        'name': [person.full_name() for _ in range(0, NUMBER_OF_USERS)],
        'age': np.random.randint(14, 90, size=NUMBER_OF_USERS)
    }
}

