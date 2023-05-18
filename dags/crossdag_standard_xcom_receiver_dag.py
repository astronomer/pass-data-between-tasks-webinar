"""
### DAG pulling XCom from another DAG

This DAG pulls two XCom pushed from within another DAG.
"""


from airflow.decorators import dag, task
from pendulum import datetime


@dag(start_date=datetime(2023, 3, 27), schedule=None, catchup=False, tags=["TaskFlow"])
def crossdag_standard_xcom_receiver_dag():
    @task
    def receiver_task(**context):
        # pull values from XCom that were created by another DAG
        # setting include_prior_dates = True is necessary unless sender and receiver DAG have the exact same execution datetime
        xcom_one = context["ti"].xcom_pull(
            dag_id="crossdag_standard_xcom_sender_dag",
            task_ids="sender_task",
            key="explicit_hello",
            include_prior_dates=True,
        )
        xcom_two = context["ti"].xcom_pull(
            dag_id="crossdag_standard_xcom_sender_dag",
            task_ids="sender_task",
            include_prior_dates=True,
        )  # the default key is 'return_value'

        print(xcom_one)
        print(xcom_two)

    receiver_task()


crossdag_standard_xcom_receiver_dag()
