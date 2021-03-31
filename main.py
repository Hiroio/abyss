import ray
import modin.pandas as pd
import numpy as np
ray.init()
people = pd.read_csv('peoples.csv', sep='\t', na_values=r'\N')

pd.set_option('display.width', 640)
pd.set_option("display.max_columns", 25)


def first_version(df, service_id, **default):
    df['service_id'] = service_id
    columns = {'tie': 0, 'paid_lastmonth': 0, 'paid_currentmonth': 0, 'charge': 0, 'subsidy': 0}
    columns.update(default)
    df = df.assign(**columns)
    df['person_id'] = df['person_id'].astype("Int64")
    df.to_csv(f'abyss_{service_id}.csv', sep='\t', na_rep=r'\N', index=False)


def second_version(schema, street_id, locality_id, buildings, service_id, people_dovid, **defaults):
    street_df = people_dovid[(people_dovid['street_id'] == street_id) & (people_dovid['city_id'] == locality_id)]
    df = street_df[street_df['building'].isin(buildings)]

    def foo(row):
        try:
            return str(row.city_id) + str(row.pid)
        except:
            return None

    df["person_id"] = df.apply(foo, axis=1)
    df['schema'] = schema
    df['locality_id'] = locality_id
    df['service_id'] = service_id
    columns = {'person_street_id': None, 'person_id_internal' : None, 'person_id_internal_text': None, 'person_id_internal_array': None, 'tie': 0, 'paid_lastmonth': 0, 'charge': 0,
               'subsidy': 0, 'paid_currentmonth': 0, 'recommended': 0, 'meters_readings': None, 'payment_details': None}
    columns.update(defaults)
    df = df.assign(**columns)
    print(df)
    df = df[['schema', 'locality_id', 'service_id', 'person_id', 'person_street_id', 'person_id_internal', 'person_id_internal_text', 'person_id_internal_array', 'tie', 'paid_lastmonth', 'charge', 'subsidy', 'paid_currentmonth', 'recommended', 'date', 'meters_readings', 'payment_details']]
    print(df)
    df.to_csv(f'abyss_{service_id}.csv', sep='\t', index=False, na_rep=r'\N')



# second_version('volyn', 1, 10001, [1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30, 31, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43, 44, 46, 47, 48, 50, 51, 52, 53, 54, 55, 56, 57,
#                                    58, 59, 60, 61, 62, 64, 66, 67, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 85,
#                                    87, 88, 90, 91, 92, 93, 94, 95, 96, 98, 102, 104, 106], 21, people, **{'date': "21-21-2001"} )


variable = pd.read_csv('1_push.csv', sep='\t', na_values=r'\N')
variable = variable[variable['locality_id'] == 10003]

first_version(variable, 31, **{'date': "21-21-2001"})

