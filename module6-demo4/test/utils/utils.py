from datetime import datetime
from typing import Callable, Any
import logging
import sys

from airflow import DAG
from airflow.models import TaskInstance


def execute_operator_in_template_context(name_of_test: str, operator_to_test: Callable, internal_arguments: dict,
                                         task_id: str = 'mock_operator', verbose: bool = False) -> Any:
    dag = DAG(dag_id=name_of_test, start_date=datetime.now())
    task = operator_to_test(dag=dag,
                            task_id=task_id,
                            **internal_arguments
                            )
    ti = TaskInstance(task=task, execution_date=datetime(2000, 12, 12))

    # n.b. when set to true, you can see the logs from your operator tests
    if verbose:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        ti._log.addHandler(stdout_handler)

    result = task.execute(context=ti.get_template_context())
    return result


def poke_sensor_in_template_context(name_of_test: str, sensor_to_test: Callable, internal_arguments: dict,
                                    task_id: str = 'mock_sensor', verbose: bool = False) -> Any:
    dag = DAG(dag_id=name_of_test, start_date=datetime.now())
    task = sensor_to_test(dag=dag,
                          task_id=task_id,
                          **internal_arguments
                          )
    ti = TaskInstance(task=task, execution_date=datetime(2000, 12, 12))

    # n.b. when set to true, you can see the logs from your operator tests
    if verbose:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        ti._log.addHandler(stdout_handler)

    return task.poke(context=ti.get_template_context())
