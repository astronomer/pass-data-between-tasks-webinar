"""
### Pull specific XCom from a dynamically mapped task

This DAG shows how you can pull specific XCom from a dynamically mapped task, as well
as how you can pull XCom from two different tasks at the same time.
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
def pull_from_several_senders():
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

    # ------------- #
    # Sender task 2 #
    # ------------- #

    @task
    def sender_task_2():
        return [1, 2, 3, 4, 5]

    sender_obj_2 = sender_task_2()

    # ------------------------- #
    # Traditional Receiver task #
    # ------------------------- #

    receiver_task_traditional = BashOperator(
        task_id="receiver_task_traditional",
        bash_command="echo {{ ti.xcom_pull(task_ids=['mapped_sender_task', 'sender_task_2'], key='return_value', map_indexes=[-1,0,1]) }}",
    )

    # ---------------------- #
    # TaskFlow Receiver task #
    # ---------------------- #

    @task
    def receiver_task_taskflow(input1, input2):
        print(input1, input2)

    [mapped_sender_obj, sender_obj_2] >> receiver_task_traditional
    receiver_task_taskflow(mapped_sender_obj, sender_obj_2)


pull_from_several_senders()
