"""
### DAG pushing XCom to be pulled from another DAG

This DAG pushes two XCom to be pulled from another DAG.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(start_date=datetime(2023, 3, 27), schedule=None, catchup=False, tags=["TaskFlow"])
def crossdag_standard_xcom_sender_dag():
    @task
    def sender_task(**context):
        today_date = context["ds"]
        # push values to XCom explicitly with a specific key by using the .xcom_push method
        # of the task instance (ti) in the Airflow context
        context["ti"].xcom_push(
            key="explicit_hello",
            value=f"I was pushed explicitly from the sender_task in the sender_dag at {today_date}!",
        )

        # the return value gets pushed to XCom implicitly (with the key 'return_value')
        return "I was returned from the sender_task in the sender_dag"

    sender_task()


crossdag_standard_xcom_sender_dag()
