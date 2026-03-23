## **1. Цель проекта**

Получить представление о совместной работе DWH, Data Lake и Data Orchestrator на примере Greenplum \ Spark \ Airflow, приближенное к реальной работе Data Engineer

**Состав проекта:** 

- пять запросов на Spark, объединенных в один DAG Airflow
- пять запросов на Greenplum, объединенных в один DAG Airflow

**Образовательные результаты проекта:** 

- использование изученных инструментов в связке: Airflow, Spark, Greenplum
- понимание, как строятся пайплайны обработки данных в озере данных и DWH
- построение отчётов DE по ТЗ от заказчика

## **2. Техническая инфраструктура**

В качестве технической инфраструктуры будут использованы следующие инструменты:

1.  Озеро данных на базе S3
2.  Аналитическое хранилище на базе Greenplum
3. Оркестратор потоков данных - Airflow  
4. Кластер распределенных вычислений Spark (Kubernetes)
5. GitLab для хранения исходного кода

Интеграция между GP и Spark будет проходить через 
внешние таблицы с хранением в S3.


## **3. Итоговый вид DAG**

Ниже представлен итоговый вид DAG'a, который у нас должен получиться.

Вам понадобятся следующие операторы и сенсоры:

-  **airflow.operators.empty.EmptyOperator** - для объединения задач
- **airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator** - для отправки Spark задач на кластер  
- **airflow.providers.cncf.kubernetes.sensors.spark_kubernetes.SparkKubernetesSensor** - для отслеживания статуса Spark задач  
- **airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator** - для выполнения запросов к Greenplum

![](https://storage.yandexcloud.net/klms-public/production/learning-content/236/2398/21993/64173/299485/KarpovDEProject-1part-ArchConfigure.drawio_gRCHtGB.png)

## **4. Файлы**

```
└── dags
    ├── de-project-user-dag.py
    ├── spark-job-customers.py
    ├── spark-job-lineitems.py
    ├── spark-job-orders.py
    ├── spark-job-parts.py
    ├── spark-job-suppliers.py
    ├── spark_submit_customers.yaml
    ├── spark_submit_lineitems.yaml
    ├── spark_submit_orders.yaml
    ├── spark_submit_parts.yaml
    ├── spark_submit_suppliers.yaml
    ├── sql-query-customers.sql
    ├── sql-query-lineitems.sql
    ├── sql-query-orders.sql
    ├── sql-query-parts.sql
    └── sql-query-suppliers.sql
```

## **5. ETL pipeline**

Мы уже научились создавать и заходить в **s3 bucket**, с помощью `aws` и запускать **spark-job** на кластере с `spark-submit`. Для этой задачи нам потребуется это сделать через **pyspark** и автоматизации **airflow**

Нужно будет (1) **прочитать данные** из папки `de-raw` для конкретного подзапроса, (2) **обработать их**, и (3) **записать агрегацию** в тот же бакет. Таких процессов у нас 5 штук, соответсвенно автоматизации процесса ETL будет уместным.

Главное в этой задаче, по большому счету это настройка всех соединении и налаживание самого процесса ETL всех компонентов (S3,PySpark,Greenplum,kubernetes) и все это у нас будет связывать **airflow**

### **Содержание S3**

Исходные данные хронятся в `s3://de-raw/`

```
root@d4a200ef8c54:/# aws --profile=karpov-user-project --endpoint-url=https://hb.bizmrg.com s3 ls s3://de-raw/
                           PRE backup/
                           PRE customer/
                           PRE lineitem/
                           PRE nation/
                           PRE orders/
                           PRE part/
                           PRE partsupp/
                           PRE region/
                           PRE supplier/
```

и хранит в себе один конкретный `parquet`, например для `nation`

```
root@d4a200ef8c54:/# aws --profile=karpov-user-project --endpoint-url=https://hb.bizmrg.com s3 ls s3://de-raw/nation/
2024-10-19 10:10:02          0 
2024-10-19 10:10:02       5540 nation.parquet
```

### **Полключение spark к S3**

В **pyspark** мы может указать настройки подключения к S3 в `SparkSession`, чтобы можно было работать с этим бакетом.

```python
def _spark_session(): 
	return (SparkSession.builder
	.appName("SparkJobExample-" + uuid.uuid4().hex) 
	.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2")
	.config('spark.hadoop.fs.s3a.endpoint', "https://")
	.config('spark.hadoop.fs.s3a.region', "region")
	.config('spark.hadoop.fs.s3a.access.key', "key")
	.config('spark.hadoop.fs.s3a.secret.key', "secret_key")
	.getOrCreate())
```

### **Запросы обработки данных**

Настроив **SparkSession** для каждого из **5 под-запросов** мы в **airflow** будем запускать spark-job на **kubernetes** кластере, для этого используем `SparkKubernetesOperator`

Все 5 подзапросов в нашем airflow таске будем запускать независимо друг от друга:

- `spark-job-customers.py`
- `spark-job-line items.py`
- `spark-job-orders.py`
- `spark-job-suppliers.py`
- `spark-job-parts.py`

### **Запуск spark-job**

Для запуска pyspark job на kubernetes кластере `kubernetes_karpov`, используется airflow оператор `SparkKubernetesOperator` с указанием пути `application_file` для конфигурационного `.yaml` фаила

```python
def _build_submit_operator(task_id: str, application_file: str, link_dag: DAG):
	
    return SparkKubernetesOperator(
        task_id=task_id,
        namespace='de-project',
        application_file=application_file,
        kubernetes_conn_id='kubernetes_karpov',
        do_xcom_push=True,
        dag=link_dag
    )
```

Для каждого из 5 spark-job используется шаблонный конфигурационный файл, в котором мы меняем только название скрипта `mainApplicationFile` и `metadata.name`

```python
# User guide https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-job-user-customers
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: itayb/spark:3.1.1-hadoop-3.2.0-aws
  imagePullPolicy: Always
  mainApplicationFile: "local:///de-project/dags/user/spark-job-customers.py"
  sparkVersion: "3.1.1"
  timeToLiveSeconds: 40
  restartPolicy:
    type: Never
  volumes:
...
```

