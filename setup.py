from setuptools import find_packages, setup

setup(
    name="pg_data_etl",
    packages=find_packages(),
    version="0.1.0",
    description="ETL tools for postgres data, built on top of the psql and pg_dump command line tools.",
    author="Aaron Fraint, AICP",
    license="GPL-3.0",
    entry_points="""
        # [console_scripts]
        # sidewalk=sidewalk_gaps.cli:main
        # transit=transit_access.cli:main
    """,
)
