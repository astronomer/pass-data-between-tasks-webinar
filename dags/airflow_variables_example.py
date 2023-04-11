from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Airflow Variables", "TaskFlow"],
)
def airflow_variables_example():
    @task
    def set_airflow_variable():
        Variable.set("MY_DINNER", "Chocolate")
        """Airflow Variables are useful for short values that rarely changed and 
        have to be accessed by several DAGs and Dagruns."""

    @task
    def get_airflow_variable():
        my_food = Variable.get("MY_DINNER")
        return my_food

    set_airflow_variable() >> get_airflow_variable()


airflow_variables_example()
