from airflow.decorators import dag, task
from airflow.configuration import conf
from pendulum import datetime


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["@task.kubernetes", "TaskFlow"],
)
def taskflow_kpo_example():
    @task
    def upstream_task():
        return [1, 2, 3]

    @task.kubernetes(
        image="python",
        name="airflow-test-pod-1",
        task_id="kpo_task",
        namespace=conf.get("kubernetes", "NAMESPACE"),
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        # make sure XComs are pushed
        do_xcom_push=True,
    )
    def kpo_task(input):
        output = int(sum(input))
        return output

    """
    See also this blog post:
    https://medium.com/apache-airflow/passing-data-between-tasks-with-the-kubernetespodoperator-in-apache-airflow-7ae9e3e6675c
    """

    @task
    def downstream_task(output):
        return "Fresh from the virtual env, the result is: " + f"{output}"

    downstream_task(kpo_task(upstream_task()))


taskflow_kpo_example()
