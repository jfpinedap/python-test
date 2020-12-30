"""ETL funtions"""

# base imports
import sys
import numpy as np
import pandas as pd
import multiprocessing as mp

# Util imports
from .utils import timeit


CHUNKSIZE = 5000  # processing 5000 rows from the file at a time

OUTPUT_FILES = 'output/'

COLUMNS = [
    'fiscal_id', 'first_name', 'last_name', 'gender', 'birth_date',
    'due_date', 'due_balance', 'address', 'ocupation', 'altura',
    'peso', 'email', 'status', 'phone', 'priority'
]

COLUMNS_COSTUMERS = [
    'fiscal_id', 'first_name', 'last_name', 'gender', 'birth_date',
    'age', 'age_group',  'due_date', 'delinquency', 'due_balance',
    'address', 'ocupation', 'best_contact_ocupation'
]

COLUMNS_EMAILS = ['fiscal_id', 'email', 'status', 'priority']

COLUMNS_PHONES = ['fiscal_id', 'phone', 'status', 'priority']

WIDTHS = [8, 20, 25, 9, 10, 10, 6, 50, 30, 4, 2, 50, 8, 9, 1]


def transform_df(df):
  """Transformation process data frame using Pandas."""

  # Assign columns to dataframe
  df.columns = COLUMNS

  # Drop unused data
  df.drop(['altura', 'peso'], axis=1, inplace=True)
  
  # Transform Date data
  now = pd.Timestamp('now')
  df['birth_date'] = pd.to_datetime(df['birth_date'], format='%Y-%m-%d')
  df['due_date'] = pd.to_datetime(df['due_date'], format='%Y-%m-%d')
  df['birth_date'] = df['birth_date'].where(
    df['birth_date'] < now, df['birth_date'] -  np.timedelta64(100, 'Y')
  )
  df['age'] = (now - df['birth_date']).astype('timedelta64[Y]')
  df['age_group']=pd.cut(
    df['age'], 
    bins=[0, 20, 30, 40, 50, 60, sys.maxsize],
    labels=[1, 2, 3, 4, 5, 6]
  )
  df['delinquency'] = (now - df['due_date']).astype('timedelta64[D]')
  
  # Fill empty data
  values = {'phone': '0', 'priority': 0, 'email': ''}
  df.fillna(value=values, inplace=True)
  
  # Data normalization 
  df['first_name'] = df['first_name'].str.upper()
  df['last_name'] = df['last_name'].str.upper()
  df['phone'] = df['phone'].astype('int64').astype('str')
  df['phone'] = df['phone'].str.upper()
  df.loc[df.phone == '0', 'phone'] = ''
  df['gender'] = df['gender'].str.upper()
  df['address'] = df['address'].str.upper()
  df['ocupation'] = df['ocupation'].str.upper()
  df['email'] = df['email'].str.lower()
  df['status'] = df['status'].str.upper()
  df['age'] = df['age'].astype('int64')
  df['age_group'] = df['age_group'].astype('int64')
  df['priority'] = df['priority'].astype('int64')
  df['delinquency'] = df['delinquency'].astype('int64')
    
  # Prepare best_contact_ocupation column
  df = df.assign(best_contact_ocupation=False,)
  return df


@timeit
def extract_transform_data(file_path):
  """
  Extract and Transformation Process.
  
  Using a pool of workers to extract and transform data from 
  Fixed Width Format files in a parallel way.
  """
  reader = pd.read_fwf(
    filepath_or_buffer=file_path,
    widths=WIDTHS,
    header=None,
    chunksize=CHUNKSIZE
  )

  pool = mp.Pool()  
  funclist = []
  for df in reader:
    # Process each data frame
    f = pool.apply_async(transform_df, [df])
    funclist.append(f)
    
  result = []
  for f in funclist:
      result.append(f.get(timeout=120)) # Timeout in 120 seconds = 2 mins

  # Combine chunks with transformed data into a single training set
  df = pd.concat(result, ignore_index=True)

  # Data normalization
  df['fiscal_id'] = df['fiscal_id'].astype('str')

  # Set the best contact by ocupation 
  grouped = df[
    df['status'] == 'VALIDO'
  ].groupby(
    ["ocupation", "fiscal_id"]
  )["ocupation"].count().reset_index(name="count")

  best_contact_ocupation = list(
    grouped.iloc[
      grouped.groupby('ocupation')['count'].agg(pd.Series.idxmax)
    ].fiscal_id
  )
  df.loc[
      df['fiscal_id'].isin(best_contact_ocupation),
      'best_contact_ocupation'
  ] = True

  # Split entities: cumtomers, emails and phones
  df_customers = df.loc[:, COLUMNS_COSTUMERS]
  df_customers = df_customers.drop_duplicates(
    subset="fiscal_id",
    keep='first'
  )

  df_emails = df.loc[df['email']!='', COLUMNS_EMAILS]
  df_emails = df_emails.drop_duplicates(
    subset=["fiscal_id", "email"],
    keep='first'
  )

  df_phones = df.loc[df['phone']!='', COLUMNS_PHONES]
  df_phones = df_phones.drop_duplicates(
    subset=["fiscal_id", "phone"],
    keep='first'
  )

  dfs = {
    'df_customers': df_customers,
    'df_emails': df_emails,
    'df_phones': df_phones
  }
  return dfs


def load_db(df, table_name, engine):
  """Load Process using Pandas."""
  df.to_sql(
    table_name,
    con=engine,
    if_exists='append',
    index=False,
    chunksize=CHUNKSIZE
  )


def save_file(df, name, columns):
  """Save xlsx files using Pandas."""
  df.to_excel(
    '{0}{1}.xlsx'.format(OUTPUT_FILES, name),
    sheet_name=name,
    index=False,
    columns=columns
  )


@timeit
def load_to_database(dfs, engine):
  """Load to database process."""
  load_db(
    df=dfs.get('df_customers'),
    table_name='customers',
    engine=engine
  )
  load_db(
    df=dfs.get('df_emails'),
    table_name='emails',
    engine=engine
  )
  load_db(
    df=dfs.get('df_phones'),
    table_name='phones',
    engine=engine
  )


@timeit
def save_xlsx_files(dfs):
  """save XLSX files process."""
  save_file(
    df=dfs.get('df_customers'),
    name='customers',
    columns=COLUMNS_COSTUMERS
  )
  save_file(
    df=dfs.get('df_emails'),
    name='emails',
    columns=COLUMNS_EMAILS
  )
  save_file(
    df=dfs.get('df_phones'),
    name='phones',
    columns=COLUMNS_PHONES
  )
