"""
### Use XCom with the @task.external_python decorator

This DAG runs a simple task within the decorator version of the ExternalPythonOperator
using an upstream value via XCom and returning another value to XCom.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 5, 1),
    schedule=None,
    catchup=False,
    tags=["@task.external_python", "TaskFlow"],
)
def epo_example():
    @task
    def upstream_task():
        return [1, 2, 3]

    @task.external_python(python="/home/astro/.pyenv/versions/numpy_env/bin/python")
    def external_python_operator_task(input):
        import numpy as np

        print(input)

        output = np.sum(input)

        return int(output)

    @task
    def downstream_task(output):
        return "Fresh from the virtual env, the result is: " + f"{output}"

    downstream_task(external_python_operator_task(upstream_task()))


epo_example()
