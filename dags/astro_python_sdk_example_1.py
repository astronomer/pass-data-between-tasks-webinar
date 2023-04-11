from airflow.decorators import dag
from pendulum import datetime
import pandas as pd

# Import decorators and classes from the SDK
from astro import sql as aql
from astro.sql.table import Table


@dag(
    start_date=datetime(2023, 3, 27),
    schedule=None,
    catchup=False,
    tags=["Astro Python SDK", "Pandas", "webinar"],
)
def astro_python_sdk_example_1():
    @aql.dataframe
    def create_df(contents_col3):
        my_dict = {
            "col1": [1, 2, 3],
            "col2": ["hi", "bye", "hello"],
            "col3": contents_col3,
        }
        df = pd.DataFrame(my_dict)
        return df

    full_table = create_df(
        contents_col3=[10, 100, 1000],
        output_table=Table(conn_id="postgres_conn"),
    )

    @aql.transform
    def select_col1_and_col3(in_table):
        return "SELECT col1, col3 FROM {{ in_table }};"

    col1_and_col3 = select_col1_and_col3(
        full_table, output_table=Table(conn_id="postgres_conn")
    )

    @aql.dataframe
    def multiply(df: pd.DataFrame):

        df["multiplied_cols"] = df["col1"] * df["col3"]
        print(df)
        return df[["multiplied_cols"]]

    multiply(
        col1_and_col3,
        output_table=Table(conn_id="postgres_conn", name="reporting_table"),
    )

    aql.cleanup()


astro_python_sdk_example_1()
