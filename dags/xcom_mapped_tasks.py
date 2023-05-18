"""
### Toy DAG showing how XCom can be used with dynamically mapped tasks

This DAG contains an example for XCom use with dynamic task mapping with both
TaskFlow tasks and traditional operators.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.bash import BashOperator


@dag(
    start_date=datetime(2023, 3, 27),
    schedule=None,
    catchup=False,
    tags=["TaskFlow", "dynamic task mapping", "traditional operators"],
)
def xcom_mapped_tasks():
    # ------------------ #
    # Mapped sender task #
    # ------------------ #

    @task
    def mapped_sender_task(good_dog, **context):
        # the return value gets pushed to XCom implicitly
        # (with the key 'return_value' and the map_index of the mapped task)
        return good_dog + " deserves a treat"

    mapped_sender_obj = mapped_sender_task.expand(
        good_dog=["Avery", "Ruthie", "Peanut", "Butter"]
    )

    # --------------------------------- #
    # TaskFlow Task receiving full list #
    # --------------------------------- #

    @task
    def receiver_task(xcom_received, **context):
        print(xcom_received)

    # pass the return value of one task to the next one to implicitly use XCom
    receiver_task(mapped_sender_obj)

    # ------------------------------------------------ #
    # Pulling specific mapped XCom in a TaskFlow task  #
    # ------------------------------------------------ #

    @task
    def receiver_task_2(xcom_received, **context):
        print(xcom_received[0] + "!")

    # pass the return value of one task to the next one to implicitly use XCom
    receiver_task_2(mapped_sender_obj)

    # ------------------------------------ #
    # Traditional Task receiving full list #
    # ------------------------------------ #

    receiver_task_3 = BashOperator(
        task_id="receiver_task_3",
        bash_command="echo {{ ti.xcom_pull(task_ids='mapped_sender_task', key='return_value')}}",
    )

    # --------------------------------------------------- #
    # Pulling specific mapped XCom in a traditional task  #
    # --------------------------------------------------- #

    receiver_task_4 = BashOperator(
        task_id="receiver_task_4",
        bash_command="echo {{ ti.xcom_pull(task_ids='mapped_sender_task', key='return_value', map_indexes=[2,3]) }}",
    )

    mapped_sender_obj >> [receiver_task_3, receiver_task_4]


xcom_mapped_tasks()
