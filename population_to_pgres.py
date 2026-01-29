
from datetime import datetime
import pandas as pd
import requests
import psycopg2
import os


def grab_year(year : str, url_back : str) -> pd.DataFrame:

    # design API url
    url_front = f'https://api.census.gov/data/{year}/acs/acs5?get='
    url = url_front + url_back

    # create dataframe
    response = requests.get(url)
    data = response.json()
    df_ = pd.DataFrame(data, columns=data[0]).drop(0)

    return df_

def create_table(url_back : str) -> pd.DataFrame:

    data_sets = {y : grab_year(
        year = y,
        url_back = url_back) for y in range(2009,2024)}
    
    df_ = pd.concat(
        [df.assign(year=i) for i, df in data_sets.items()],
        ignore_index=True)

    return df_

if __name__ == '__main__' :
    
    # ---------------------------------------------------------
    # get sex population
    # ---------------------------------------------------------

    url_back = 'group(B01001)&ucgid=pseudo(0400000US17$0500000)'
    df_sex = create_table(url_back = url_back)

    rename = {
        'B01001_002E' : 'Male',
        'B01001_026E' : 'Female',
        'NAME' : 'county_name'}
    df_sex.rename(columns=rename, inplace=True)
    
    select_col = [
        'Male', 'year','Female',
        'ucgid', 'county_name']
    df_sex = df_sex[select_col]

    df_sex = df_sex.melt(id_vars=['county_name', 'ucgid', 'year'])
    df_sex['value'] = df_sex['value'].astype(int)

    # create a merge template for the race table -------------

    df_merge = df_sex[['county_name', 'ucgid']]
    where = df_merge.duplicated()
    df_merge = df_merge.loc[~where]
    df_merge['merge_code'] = df_merge['ucgid'].str.split('US17').str[1]

    # ---------------------------------------------------------
    # get race & ethnicity population
    # ---------------------------------------------------------

    map_codes = {
        'B03002_001E': 'Total',
        'B03002_003E': 'White',
        'B03002_004E': 'Black',
        'B03002_006E': 'Asian',
        'B03002_007E': 'Pacific Islander',
        'B03002_005E': 'American Indian',
        'B03002_009E': 'Bi-Racial',
        'B03002_012E': 'Hispanic'}
    codes = ','.join(list(map_codes.keys()))

    url_back = f'{codes}&for=county:*&in=state:17'
    df_race = create_table(url_back)

    df_race.rename(columns=map_codes, inplace = True)
    
    ai_values = df_race['American Indian'].astype(int)
    pi_values = df_race['Pacific Islander'].astype(int)
    df_race['American Indian'] = (ai_values + pi_values).astype(str)
    df_race.drop(columns=['Pacific Islander'], inplace=True)

    df_race = df_race.melt(id_vars=['county', 'year'])
    df_race['value'] = df_race['value'].astype(int)

    df_race = df_race.merge(
        df_merge, how = 'left',
        left_on = 'county', right_on = 'merge_code')
    df_race.drop(columns=['county', 'merge_code'], inplace=True)

    # ---------------------------------------------------------
    # combine and archive
    # ---------------------------------------------------------

    df = pd.concat([df_sex, df_race])
    df['ucgid'] = df['ucgid'].str.split('US').str[1]

    folder = '/encrypted/data/Justice_Counts/census_county_data/'

    # create a file_name that is timestamped
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f'{folder}upload_{timestamp}.csv'

    df.to_csv(file_name)

    # ---------------------------------------------------------
    # upload to pgres
    # ---------------------------------------------------------

    # retrieve credentials
    username = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')

    # if not defined default to the user
    if username == None:
        os.environ['POSTGRES_USER'] = input('ccjda username:')  
        os.environ['POSTGRES_PASSWORD'] = input('password:')

            # reset credentials
        username = os.getenv('POSTGRES_USER')
        password = os.getenv('POSTGRES_PASSWORD')

    # Connect to db
    conn = psycopg2.connect(
        dbname='archives',
        user = username,
        password = password,
        host = 'ccjda1.icjia.org',
        port="5432" )
    cursor = conn.cursor()

    pgres_name = 'justice_counts.geoid_population'
    print(f'---- Upload {pgres_name} to PostGres')

    # Truncate the table to remove existing data
    query = 'TRUNCATE TABLE ' + pgres_name
    cursor.execute(query)

    # iterate and upload each row
    for index, row in df.iterrows():
        columns = ', '.join(row.index)
        values = ', '.join(['%s'] * len(row))
        insert_query = f'INSERT INTO {pgres_name} ({columns}) VALUES ({values})'
        
        cursor.execute(insert_query, tuple(row))

    # Commit the transaction
    conn.commit()

    # Close the connection
    cursor.close()
    conn.close()