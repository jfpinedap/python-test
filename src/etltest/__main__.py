'''ETL Python Tecnical TEST\n.

This application is a simple ETL to extract data from a customer.txt file 
to load to SQLite database using Pandas to transform the data, and generate .xlsx files.
'''

# Base imports
import argparse
import sys
import os
from pathlib import Path
from pathlib import PureWindowsPath

from etltest import (
  get_engine, extract_transform_data,
  load_to_database, save_xlsx_files, 
)


def extant_file(x):
  """
  'Type' for argparse - checks that file exists but does not open.
  """
  if not os.path.exists(x):
    # Argparse uses the ArgumentTypeError to give a rejection message like:
    # error: argument input: x does not exist
    raise argparse.ArgumentTypeError("{0} does not exist".format(x))
  return x

def create_arg_parser():
  """Creates and returns the ArgumentParser object."""

  parser = argparse.ArgumentParser(
    description=__doc__
  )
  parser.add_argument(
    "-i", "--input",
    dest="filename",
    required=True,
    type=extant_file,
    metavar="FILE",
    help='Get path to the input FWF file. (.txt)'
  )
  return parser


def main(args=None):
  arg_parser = create_arg_parser()
  parsed_args = arg_parser.parse_args(sys.argv[1:])

  file_path = Path(parsed_args.filename)
  print('file_path: ', file_path)
  if not os.path.exists(file_path):
    print('The file doesn\'t exist.')
    sys.exit(0)

  # Configure database engine
  engine = get_engine()

  # Extract and transform data
  dfs = extract_transform_data(file_path=file_path)

  # Load the transform data to database
  load_to_database(dfs=dfs, engine=engine)

  # Save the transform data to XLSX files
  save_xlsx_files(dfs=dfs)

  # End ETL process
  sys.exit(0)


if __name__ == "__main__":
  main()