from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    default_args={"owner": "airflow"},
    dag_id="example_dag",
    schedule="@daily",
) as dag:
    task1 = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")


