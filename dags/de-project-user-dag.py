
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

def _build_submit_operator(task_id: str, application_file: str, link_dag: DAG):
	
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace='de-project',
        application_file=application_file,
        kubernetes_conn_id='kubernetes_karpov',
        do_xcom_push=True,
        dag=link_dag
    )

def _build_sensor(task_id: str, 
				  application_name: str, 
				  link_dag: DAG):
				  
    return SparkKubernetesSensor(
        task_id=task_id,
        namespace='de-project',
        application_name=application_name,
        kubernetes_conn_id='kubernetes_karpov',
        attach_log=True,
        dag=link_dag
    )

def _build_sql_operator(task_id: str, 
						sql: str, 
						link_dag: DAG):
						
    return SQLExecuteQueryOperator(
        task_id=task_id,
        sql=sql,
        conn_id='greenplume_karpov',
        dag=link_dag,
    )


with DAG(

    dag_id='de-project-dag',
    schedule_interval=None,
    start_date=pendulum.datetime(2026,1,1, tz='UTC'),
    tags=['my_tag'],
    template_searchpath='path',
    catchup=False
	
) as dag:

    dummy_start = EmptyOperator(task_id='start', dag=dag)
    dummy_end = EmptyOperator(task_id='end', dag=dag)

    # spark-submit
    submit_customers = _build_submit_operator(
        task_id='submit_customers',
        application_file='spark_submit_customers.yaml',
        link_dag=dag
    )

    submit_lineitems = _build_submit_operator(
        task_id='submit_lineitems',
        application_file='spark_submit_lineitems.yaml',
        link_dag=dag
    )

    submit_orders = _build_submit_operator(
        task_id='submit_orders',
        application_file='spark_submit_orders.yaml',
        link_dag=dag
    )

    submit_parts = _build_submit_operator(
        task_id='submit_parts',
        application_file='spark_submit_parts.yaml',
        link_dag=dag
    )

    submit_suppliers = _build_submit_operator(
        task_id='submit_suppliers',
        application_file='spark_submit_suppliers.yaml',
        link_dag=dag
    )

    # sensors
    sensor_customers = _build_sensor(
        task_id='sensor_customers',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{'submit_customers'}')['metadata']['name']}}}}",
        link_dag=dag
    )

    sensor_lineitems = _build_sensor(
        task_id='sensor_lineitems',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{'submit_lineitems'}')['metadata']['name']}}}}",
        link_dag=dag
    )

    sensor_orders = _build_sensor(
        task_id='sensor_orders',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{'submit_orders'}')['metadata']['name']}}}}",
        link_dag=dag
    )

    sensor_parts = _build_sensor(
        task_id='sensor_parts',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{'submit_parts'}')['metadata']['name']}}}}",
        link_dag=dag
    )

    sensor_suppliers = _build_sensor(
        task_id='sensor_suppliers',
        application_name=f"{{{{task_instance.xcom_pull(task_ids='{'submit_suppliers'}')['metadata']['name']}}}}",
        link_dag=dag
    )

    # postgres operators
    customers_datamart = _build_sql_operator(task_id='customers_datamart',sql='sql-query-customers.sql',link_dag=dag)
    lineitems_datamart = _build_sql_operator(task_id='lineitems_datamart',sql='sql-query-lineitems.sql',link_dag=dag)
    orders_datamart = _build_sql_operator(task_id='orders_datamart',sql='sql-query-orders.sql',link_dag=dag)
    parts_datamart = _build_sql_operator(task_id='parts_datamart',sql='sql-query-parts.sql',link_dag=dag)
    suppliers_datamart = _build_sql_operator(task_id='suppliers_datamart',sql='sql-query-suppliers.sql',link_dag=dag)

    # task workflow
    [
        dummy_start >> submit_customers >> sensor_customers >> customers_datamart,
        dummy_start >> submit_lineitems >> sensor_lineitems >> lineitems_datamart,
        dummy_start >> submit_orders >> sensor_orders >> orders_datamart,
        dummy_start >> submit_parts >> sensor_parts >> parts_datamart,
        dummy_start >> submit_suppliers >> sensor_suppliers >> suppliers_datamart,
    ] >> dummy_end
