"""
### Use XCom with the KubernetesPodOperator

This DAG shows a simple implementation of the KubernetsPodOperator pulling from and 
pushing to XCom. Note that you will need to run this task on a Kubernetes cluster
and additional configuration of the Docker image is needed to push to XCom.
(explained in a code comment.)
"""

from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from pendulum import datetime

YOUR_DOCKER_IMAGE = "your docker image"


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["KubernetesPodOperator", "TaskFlow"],
)
def traditional_kpo_example():
    @task
    def upstream_task():
        return [1, 2, 3]

    kpo_task = KubernetesPodOperator(
        task_id="kpo_task",
        name="airflow-test-pod-2",
        image=YOUR_DOCKER_IMAGE,
        # pass in XCom via ENV
        env_vars={"MY_VAR": "{{ ti.xcom_pull(task_ids='upstream_task')}}"},
        namespace=conf.get("kubernetes", "NAMESPACE"),
        in_cluster=True,  # if you are not running Airflow on K8s already you will need
        # to adjust the KPO parameters to connect to your cluster
        get_logs=True,
        log_events_on_failure=True,
        is_delete_operator_pod=True,
        # make sure XComs are pushed
        do_xcom_push=True,
    )

    """
    In order to push XCom from within the script in the Dockerimage back to
    your Airflow environment you will need to write them to the:
    ./airflow/xcom/return.json
    file.
    If 'do_xcom_push=True' the KPO will take the contents from this file and
    push them to XCom.

    See a full example of a Dockerimage and script doing so here:
    https://docs.astronomer.io/learn/kubepod-operator#example-use-the-kubernetespodoperator-with-xcoms
    See also this blog post:
    https://medium.com/apache-airflow/passing-data-between-tasks-with-the-kubernetespodoperator-in-apache-airflow-7ae9e3e6675c
    """

    @task
    def downstream_task(output):
        return "Fresh from the pod, the result is: " + f"{output}"

    upstream_task() >> kpo_task >> downstream_task(kpo_task.output)


traditional_kpo_example()
