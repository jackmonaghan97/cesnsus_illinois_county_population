
import pandas as pd
import requests
import duckdb


def grab_year(year : str, url_back : str) -> pd.DataFrame:

    # design API url
    url_front = f'https://api.census.gov/data/{year}/acs/acs5?get='
    url = url_front + url_back

    # create dataframe
    response = requests.get(url)
    data = response.json()
    df_ = pd.DataFrame(data, columns=data[0]).drop(0)

    return df_

def pandas_to_sql(dtype):
    
    if dtype.kind in {"i"}:      # integer
        return "INTEGER"
    
    if dtype.kind in {"f"}:      # float
        return "DOUBLE PRECISION"
    
    if dtype.kind in {"b"}:      # boolean
        return "BOOLEAN"
    
    if dtype.kind in {"M"}:      # datetime64
        return "TIMESTAMP"
    
    if dtype.name == "category":
        return "VARCHAR"
    
    return "VARCHAR"             # fallback for object/string

def generate_create_table_sql(df_ : pd.DataFrame, table_name):
    
    cols = []
    for col, dtype in df_.dtypes.items():
        sql_type = pandas_to_sql(dtype)
        cols.append(f'"{col}" {sql_type}')
    
    cols_sql = ",\n  ".join(cols)

    # upload to duckdb
    db_path = r"C:\Users\jackm\OneDrive\Documents\duckdb_cli-windows-amd64\my_database.duckdb"
    con = duckdb.connect(db_path)

    con.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} ({cols_sql});
    """)

def create_table(url_back : str) -> pd.DataFrame:

    data_sets = {y : grab_year(
        year = y,
        url_back = url_back) for y in range(2009,2024)}
    
    df_ = pd.concat(
        [df.assign(year=i) for i, df in data_sets.items()],
        ignore_index=True)

    return df_

def upload_pgres(df_ : pd.DataFrame, pgres_name : str) -> None:

    # upload to duckdb
    db_path = r"C:\Users\jackm\OneDrive\Documents\duckdb_cli-windows-amd64\my_database.duckdb"
    con = duckdb.connect(db_path)

    #### FUNCTION

    con.execute(f'TRUNCATE TABLE {pgres_name}')
    con.register('df_view', df_)

    # 3. Insert fresh data
    con.execute(f"""
        INSERT INTO {pgres_name}
        SELECT * FROM df_view;
    """)

    con.close()

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

    df = pd.concat([df_sex, df_race])

    # ---------------------------------------------------------
    # upload to pgres
    # ---------------------------------------------------------

    df = df.convert_dtypes()
    generate_create_table_sql(df, 'cesnsus_illinois_county_population')

    upload_pgres(df, 'cesnsus_illinois_county_population')
