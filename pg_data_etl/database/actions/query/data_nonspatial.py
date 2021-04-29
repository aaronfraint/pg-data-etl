import pandas as pd
import sqlalchemy


def df(self, query: str) -> pd.DataFrame:
    """
    - Return a `pandas.Dataframe` from a SQL query
    """

    engine = sqlalchemy.create_engine(self.uri)

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df
