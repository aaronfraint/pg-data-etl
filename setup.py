from setuptools import find_packages, setup

setup(
    name="pg_data_etl",
    packages=find_packages(),
    version="0.2.4",
    description="ETL tools for spatial data stored in postgres",
    author="Aaron Fraint, AICP",
    license="GPL-3.0",
    entry_points="""
        [console_scripts]
        pg=pg_data_etl.cli:main
    """,
)
