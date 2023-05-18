"""
### Toy DAG showing how to set, update and retrive an Airflow Variable programmatically

Airflow Variables are useful for short values that rarely change and have to be 
accessed by several DAGs and Dagruns. You can set them in the Airflow UI under
**Admin** -> **Variables**, in the `airflow_settings.yml`, via the `airflow variables`
CLI command or programmatically as shown in this DAG.
Learn more at: https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
"""

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
        """Sets an Airflow variable MY_DINNER if it has not been set before."""
        if Variable.get("MY_DINNER", default_var=None) == None:
            Variable.set("MY_DINNER", "Chocolate")
        else:
            Variable.update("MY_DINNER", "More Chocolate")

    @task
    def get_airflow_variable():
        my_food = Variable.get("MY_DINNER")
        return my_food

    set_airflow_variable() >> get_airflow_variable()


airflow_variables_example()
