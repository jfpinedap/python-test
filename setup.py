#!/usr/bin/env python


import setuptools


with open("README.md", "r", encoding="utf-8") as fh:
  long_description = fh.read()

setuptools.setup(
  # Metadata
  name='etltest',
  version='0.0.1',
  description=(
    'This application is a simple ETL to extract data from a customer.txt '
    'file to load to SQLite database using Pandas to transform the data.'
  ),
  long_description=long_description,
  long_description_content_type='text/markdown',
  author='jfpinedap',
  author_email='jfpinedap@gmail.com',
  include_package_data=True,
  python_requires='>=3.8',
  url='https://github.com/jfpinedap/python-test.git',
  license='MIT',
  classifiers=[
    'Development Status :: 1 - Planning',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Natural Language :: English',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
  ],

  # Dependencies
  install_requires=[
    'pandas',
    'openpyxl',
  ],

  # Contents
  packages=setuptools.find_packages('src'),
  package_dir={'': 'src'},
  entry_points={
    'console_scripts': [
      'etltest = etltest.__main__:main',
    ],
  },
)
