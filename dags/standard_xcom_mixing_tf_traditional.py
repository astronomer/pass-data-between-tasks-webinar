"""
### Toy DAG showing basic use of XCom between TaskFlow tasks and traditional operators

This DAG shows how to pass data between TaskFlow and traditional tasks using XCom.
"""

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from pendulum import datetime


@dag(
    start_date=datetime(2023, 3, 27),
    schedule=None,
    catchup=False,
    tags=["TaskFlow", "traditional operators"],
)
def standard_xcom_mixing_tf_traditional():
    # ---------------------------------------- #
    # Traditional to TaskFlow, implicit syntax #
    # ---------------------------------------- #

    sender_task_traditional = BashOperator(
        task_id="sender_task_traditional",
        bash_command='echo "Hi"',
    )

    @task
    def receiver_task_tf_one(xcom_received):
        print(xcom_received + " friend!")

    # the .output attribute of a traditional task object contains its return value XCom reference
    receiver_task_tf_one(sender_task_traditional.output)

    # ---------------------------------------- #
    # Traditional to TaskFlow, template syntax #
    # ---------------------------------------- #

    @task(
        templates_dict={
            "xcom_received": "{{ ti.xcom_pull(task_ids='sender_task_traditional', key='return_value')}}"
        }
    )
    def receiver_task_tf_two(**context):
        my_greeting = context["templates_dict"]["xcom_received"]
        print(my_greeting + " friend!")

    sender_task_traditional >> receiver_task_tf_two()

    # --------------------------------------------- #
    # Traditional to TaskFlow, explicit pull syntax #
    # --------------------------------------------- #

    @task
    def receiver_task_tf_three(**context):
        my_greeting = context["ti"].xcom_pull(
            task_ids="sender_task_traditional"
        )  # the default key is "return_value"
        print(my_greeting + " friend!")

    sender_task_traditional >> receiver_task_tf_three()

    # ----------------------- #
    # TaskFlow to traditional #
    # ----------------------- #

    @task
    def sender_task_tf():
        # the return value gets pushed to XCom implicitly (with the key 'return_value')
        return "Hello"

    receiver_task_traditional = BashOperator(
        task_id="receiver_task_traditional",
        bash_command='echo "$MY_GREETING friend!"',
        env={
            "MY_GREETING": "{{ ti.xcom_pull(task_ids='sender_task_tf', key='return_value') }}"
        },
    )

    sender_task_tf() >> receiver_task_traditional


standard_xcom_mixing_tf_traditional()
