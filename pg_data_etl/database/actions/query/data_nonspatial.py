import pandas as pd
import sqlalchemy


def df(self, query: str) -> pd.DataFrame:
    """
    - Return a `pandas.Dataframe` from a SQL query

    Arguments:
        query (str): any valid SQL query that returns tabular data

    Returns:
        pd.DataFrame: a pandas dataframe with all rows/columns from the query
    """

    engine = sqlalchemy.create_engine(self.uri)

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df
