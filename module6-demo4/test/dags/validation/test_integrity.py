"""Test integrity of dags."""

import importlib
import os
from pathlib import Path

import pytest
from airflow import models as af_models

DAG_PATH = os.path.join(
    os.getcwd(), 'dags'
)
PATHLIST = Path(DAG_PATH).glob('**/*dag.py')
DAG_FILES = [str(path) for path in PATHLIST]


@pytest.mark.parametrize('dag_file', DAG_FILES)
def test_dag_integrity(dag_file):
    """Import dag files and check for DAG."""
    module_name, _ = os.path.splitext(dag_file)
    module_path = os.path.join(DAG_PATH, dag_file)
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(module)

    dag_objects = [var for var in vars(module).values() if isinstance(var, af_models.DAG)]
    assert dag_objects  # Check that our list of modules actually have instances of DAGs

    for dag in dag_objects:
        dag.test_cycle()
