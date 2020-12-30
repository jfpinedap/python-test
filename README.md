# Technical test - ETL Python

This application is a simple ETL to extract data from a fixed-width formatted (FWF) file to load to SQLite database using Pandas to transform the data.

### Requirements 📋

This project is developed to solve an specific ETL problem. It needs the correct input data to run. 

```
  Python 3.8
```

## Get started 🚀

Clone the repository
```
  git clone https://github.com/jfpinedap/python-test.git
  cd python-test
```

Create a virtualenv and activate
```
  python3 -m venv venv
  . venv/bin/activate
```

## Install and execute the project 

```
  pip install -e .
```

```
  etltest -i <FILE>
```

  For example
```
  etltest -i ~/Downloads/customers.txt
```

