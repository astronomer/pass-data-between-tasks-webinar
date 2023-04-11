from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from pendulum import datetime


def sender_task_function(**context):

    # push values to XCom explicitly with a specific key by using the .xcom_push method 
    # of the task instance (ti) in the Airflow context
    context["ti"].xcom_push(key="my_number", value=23)

    # the return value gets pushed to XCom implicitly with the key 'return_value'
    return "Avery"


def receiver_task_function(xcom_received, **context):

    # pull values from XCom explicitly with a specific key by using the .xcom_pull method
    my_number = context["ti"].xcom_pull(key="my_number")

    print(xcom_received + f" deserves {my_number} treats!")


@dag(start_date=datetime(2023, 3, 27), schedule=None, catchup=False, tags=["traditional operators"])
def standard_xcom_traditional():

    sender_task = PythonOperator(
        task_id="sender_task",
        python_callable=sender_task_function,
    )

    receiver_task = PythonOperator(
        task_id="receiver_task",
        python_callable=receiver_task_function,
        op_kwargs={
            # pull values from XCom using a Jinja template. Note that the parameter is called task_ids not task_id
            "xcom_received": "{{ ti.xcom_pull(task_ids='sender_task', key='return_value')}}"
        },
    )

    sender_task >> receiver_task


standard_xcom_traditional()
