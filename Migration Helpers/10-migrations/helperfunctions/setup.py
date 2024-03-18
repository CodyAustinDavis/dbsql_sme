from setuptools import setup

setup(
    name='helperfunctions',
    version='2.0.1',
    description='Lakehouse Warehousing and Delta Helper Functions',
    author='Cody Austin Davis @Databricks, Inc.',
    author_email='cody.davis@databricks.com',
    py_modules=['datavalidator', 
              'dbsqltransactions', 
              'stmvorchestrator', 
              'redshiftchecker', 
              'dbsqlclient', 
              'transactions',
              'deltalogger',
              'deltahelpers',
              'deltaoptimizer'],
    install_requires=[
        'sqlparse',
        'sql_metadata',
        'sqlglot',
        'pyarrow'
    ]
)