import modin.pandas as pd
import numpy as np
import ray
ray.init()

pd.set_option('display.width', 640)
pd.set_option("display.max_columns", 25)
buildings = [1, 2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18, 19, 20, 21, 22, 24, 25, 26, 27, 28, 29, 30, 31, 33, 34, 36, 37, 38, 39, 40, 41, 42, 43, 44, 46, 47, 48, 50, 51, 52, 53, 54, 55, 56, 57,
                                   58, 59, 60, 61, 62, 64, 66, 67, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 85,
                                   87, 88, 90, 91, 92, 93, 94, 95, 96, 98, 102, 104, 106]

people_dovid = pd.read_csv('peoples.csv', sep='\t', na_values=r'\N')

street_df = people_dovid[(people_dovid['street_id'] == 1) & (people_dovid['city_id'] == 10001)]
df = street_id[street_id['building'].isin(buildings)]
print (df)

print(list(set(street_df['building'])))