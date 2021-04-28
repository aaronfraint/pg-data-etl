import pandas as pd
import sqlalchemy


def get_df(db, query: str) -> pd.DataFrame:
    """
    - Return a `pandas.Dataframe` from a SQL query
    """

    engine = sqlalchemy.create_engine(db.uri)

    df = pd.read_sql(query, engine)

    engine.dispose()

    return df
