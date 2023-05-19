"""
### Dynamically map over another dynamically mapped task

This DAG contains an example for serial dynamic task mapping both using 
TaskFlow and traditional operators. 
Learn more at: https://docs.astronomer.io/learn/dynamic-tasks#repeated-mapping
"""

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from pendulum import datetime


@dag(
    start_date=datetime(2023, 3, 27),
    schedule=None,
    catchup=False,
    tags=["traditional operators", "TaskFlow"],
)
def repeated_mapping_xcom():
    # -------- #
    # TaskFlow #
    # -------- #

    @task
    def multiply_by_2_tf(num):
        return num * 2

    @task
    def add_10_tf(num):
        return num + 10

    @task
    def multiply_by_100_tf(num):
        return num * 100

    multiply_by_100_tf.expand(
        num=add_10_tf.expand(num=multiply_by_2_tf.expand(num=[1, 2, 3]))
    )

    # --------------------- #
    # Traditional operators #
    # --------------------- #

    def multiply_by_2_func(num):
        return [num * 2]

    def add_10_func(num):
        return [num + 10]

    def multiply_by_100_func(num):
        return num * 100

    multiply_by_2 = PythonOperator.partial(
        task_id="multiply_by_2", python_callable=multiply_by_2_func
    ).expand(op_args=[[1], [2], [3]])

    add_10 = PythonOperator.partial(
        task_id="add_10", python_callable=add_10_func
    ).expand(op_args=multiply_by_2.output)

    multiply_by_100 = PythonOperator.partial(
        task_id="multiply_by_100", python_callable=multiply_by_100_func
    ).expand(op_args=add_10.output)

    multiply_by_2 >> add_10 >> multiply_by_100


repeated_mapping_xcom()
