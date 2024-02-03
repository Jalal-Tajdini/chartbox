from randomuser import RandomUser
import pandas as pd
import hashlib
import psycopg2
from database.controller import Controller


# Get random users info
n_users = 10
user_list = RandomUser.generate_users(n_users)

# Form a dataframe to store all data
for user_index, user in enumerate(user_list):
    new_user_dict = user._data
    new_user_df = pd.json_normalize(new_user_dict)
    if user_index == 0:
        columns = new_user_df.columns
        all_users_df = pd.DataFrame(index=range(n_users), columns=columns)

    all_users_df.loc[user_index] = new_user_df.loc[0]

# Filter over 30 y/o men
mask = (all_users_df['gender'] == 'male') & (all_users_df['dob.age'] > 30)
men_over_thiry_df = all_users_df[mask]

# Hash passwords (there are already some columns that have done it, but i wont use them)
column_to_hash = 'login.password'
all_users_df[column_to_hash] = all_users_df[column_to_hash].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())

# change column names
columns = [column.replace('.', '_') for column in all_users_df.columns]
all_users_df.columns = columns

# Create a new dataframe
controller = Controller()
controller.create_new_db(new_db_name='chartbox', close_connection=False)
controller.create_initial_tables(db_name='chartbox', dataframe=all_users_df.iloc[[0]], close_connection=False)
controller.import_to_db(table_name='data_table', dataframe=all_users_df, close_connection=True)



