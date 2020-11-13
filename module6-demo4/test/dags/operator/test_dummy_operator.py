from airflow.operators.dummy_operator import DummyOperator

from test.utils.utils import execute_operator_in_template_context


def test_execute():
    execute_operator_in_template_context(name_of_test='test_dummy_operator',
                                         operator_to_test=DummyOperator,
                                         task_id='mock_dummy_operator',
                                         internal_arguments={}
                                         )
