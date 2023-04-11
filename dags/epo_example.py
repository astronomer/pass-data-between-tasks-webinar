from airflow.decorators import dag, task
import pendulum


@dag(
    start_date=pendulum.datetime(2022, 10, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["@task.external_python", "TaskFlow"],
)
def epo_example():
    @task
    def upstream_task():
        return [1, 2, 3]

    @task.external_python(
        python='/home/astro/.pyenv/versions/numpy_env/bin/python'
    )
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
