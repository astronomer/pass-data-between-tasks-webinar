from airflow.decorators import dag, task
import pendulum


@dag(
    start_date=pendulum.datetime(2022, 10, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["@task.virtualenv", "TaskFlow"],
)
def python_virtual_env_example():
    @task
    def upstream_task():
        return [1, 2, 3]

    @task.virtualenv(
        task_id="virtualenv_python",
        python_version="3.8.4",
        requirements=["numpy==1.24.2"],
        system_site_packages=True,
    )
    def python_virtual_env_operator_task(input):

        import numpy as np

        output = np.sum(input)

        return int(output)

    @task
    def downstream_task(output):
        return "Fresh from the virtual env, the result is: " + f"{output}"

    downstream_task(python_virtual_env_operator_task(upstream_task()))


python_virtual_env_example()
