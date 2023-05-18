"""
### Toy DAG showing explicit and implicit use of XCom with TaskFlow tasks

This DAG shows how to pass data between TaskFlow tasks using XCom.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    start_date=datetime(2023, 3, 27),
    schedule=None,
    catchup=False,
    tags=["webinar", "TaskFlow"],
)
def standard_xcom_taskflow():
    @task
    def sender_task(**context):
        # push values to XCom explicitly with a specific key by using the .xcom_push method
        # of the task instance (ti) in the Airflow context
        context["ti"].xcom_push(key="my_number", value=3)

        # the return value gets pushed to XCom implicitly (with the key 'return_value')
        return "Avery"

    @task
    def receiver_task(xcom_received, **context):
        # pull values from XCom explicitly with a specific key by using the .xcom_pull method
        my_number = context["ti"].xcom_pull(key="my_number")

        print(xcom_received + f" deserves {my_number} treats!")

    # pass the return value of one task to the next one to implicitly use XCom
    receiver_task(sender_task())


standard_xcom_taskflow()
