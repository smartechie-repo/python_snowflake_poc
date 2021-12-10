from setuptools import setup, find_packages

setup(
    name="Python Snowflake Poc",
    version="0.0.1",
    description="Python Snowflake Poc",
    entry_points='''
    [console_scripts]
    python_snowflake_poc=python_snowflake_poc:web
    ''',
    py_modules=['src'],
    scripts=['main.py']
)   
