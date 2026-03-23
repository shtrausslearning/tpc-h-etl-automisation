## **Обработка данных для отчета по LineItems**

**Скрипт**

[spark-job-lineitems.py](https://github.com/shtrausslearning/tpc-h-etl-automisation/blob/main/dags/spark-job-lineitems.py)

**Описание задачи**

Необходимо построить отчет по данным о позициях в заказе (lineitems), содержащий сводную информацию по позициям каждого заказа, когда-либо совершенного в системе, группировать данные необходимо по идентификатору заказа.  
Используйте сортировку по ключу заказа на возрастание.

**Итоговый формат**

В отчете должны присутствовать следующие колонки:

|   |   |   |
|---|---|---|
|**Поле**|**Тип**|**Описание**|
|L_ORDERKEY|bigint|уникальный идентификатор заказа|
|count|int|число позиций/вещей в заказе|
|sum_extendprice|float|сумма заказа (сумма всех позиций по L_EXTENDEDPRICE)|
|mean_discount|float|медиана скидок по позициям (L_DISCOUNT)|
|mean_tax|float|средний налог в заказе между позициями|
|delivery_days|float|среднее время доставки всех позиций заказа (в днях) с момента заказа (L_RECEIPTDATE) до момента доставки (L_SHIPDATE)|
|A_return_flags|int|количество заказов с флагом L_RETURNFLAG == 'A'|
|R_return_flags|int|количество заказов с флагом L_RETURNFLAG == 'R'|
|N_return_flags|int|количество заказов с флагом L_RETURNFLAG == 'N'|

**Пример результата**

![](https://storage.yandexcloud.net/klms-public/production/learning-content/236/2398/22154/63866/396938/image.png)

**Исходные данные**

Используйте данные:

s3a://de-raw/lineitem

**Итоговое расположение и формат**

Разместите отчет в формате parquet по пути:

s3a://de-project/{USER_PATH}/lineitems_report


## **Обработка данных для отчета по Orders**

**Скрипт**

[spark-job-orders.py](https://github.com/shtrausslearning/tpc-h-etl-automisation/blob/main/dags/spark-job-orders.py)

**Описание задачи**

Необходимо построить отчет по данным о заказах (orders), содержащий сводную информацию по заказам в разрезе месяца, страны клиента, оформляющего заказ, а также приоритета выполняемого заказа.   
Используйте сортировку по названию страны и приоритета заказа на возрастание.

**Итоговый формат**

В отчете должны присутствовать следующие колонки:

|   |   |   |
|---|---|---|
|**Поле**|**Тип**|**Описание**|
|O_MONTH|str|год и месяц заказа (O_ORDERDATE) в формате YYYY-MM|
|N_NAME|str|название страны заказа|
|O_ORDERPRIORITY|str|приоритет заказа (O_ORDERPRIORITY)|
|orders_count|int|число заказов|
|avg_order_price|float|средняя цена в заказе (O_TOTALPRICE)|
|sum_order_price|float|сумма заказов (O_TOTALPRICE)|
|min_order_price|float|минимальная цена в заказе (O_TOTALPRICE)|
|max_order_price|float|максимальная цена в заказе (O_TOTALPRICE)|
|f_order_status|int|количество заказов с флагом O_ORDERSTATUS == 'F'|
|o_order_status|int|количество заказов с флагом O_ORDERSTATUS == 'O'|
|p_order_status|int|количество заказов с флагом O_ORDERSTATUS == 'P'|

**Пример результата**

![](https://storage.yandexcloud.net/klms-public/production/learning-content/236/2398/22154/63866/396939/image.png)

**Исходные данные**

Используйте данные:

s3a://de-raw/orders s3a://de-raw/customer s3a://de-raw/nation

**Итоговое расположение и формат**

Разместите отчет в формате parquet по пути:

s3a://de-project/{USER_PATH}/orders_report


## **Обработка данных для отчета по Customers**

**Скрипт**

[spark-job-customers.py](https://github.com/shtrausslearning/tpc-h-etl-automisation/blob/main/dags/spark-job-customers.py)

**Описание задачи**

Необходимо построить отчет по данным о клиентах (customers), содержащий сводную информацию по заказам в разрезе страны, откуда был отправлен заказ, а также приоритета выполняемого заказа.   
Используйте сортировку по названию страны (N_NAME) и приоритета заказа (C_MKTSEGMENT) на возрастание.

**Итоговый формат**

В отчете должны присутствовать следующие колонки:

|   |   |   |
|---|---|---|
|**Поле**|**Тип**|**Описание**|
|R_NAME|str|название региона|
|N_NAME|str|название страны|
|C_MKTSEGMENT|str|маркетинговый сегмент|
|unique_customers_count|int|число уникальных заказов|
|avg_acctbal|float|средний остаток клиента (C_ACCTBAL)|
|mean_acctbal|float|медианный остаток клиента (C_ACCTBAL)|
|min_acctbal|float|минимальный остаток клиента (C_ACCTBAL)|
|max_acctbal|float|максимальный остаток клиента (C_ACCTBAL)|

**Пример результата**

![](https://storage.yandexcloud.net/klms-public/production/learning-content/236/2398/22154/63866/396940/image.png)

**Исходные данные**

Используйте данные:

s3a://de-raw/customer s3a://de-raw/nation s3a://de-raw/region

**Итоговое расположение и формат**

Разместите отчет в формате parquet по пути:

s3a://de-project/{USER_PATH}/customers_report


## **Обработка данных для отчета по Suppliers**

**Скрипт**

[spark-job-suppliers.py](https://github.com/shtrausslearning/tpc-h-etl-automisation/blob/main/dags/spark-job-suppliers.py)

**Описание задачи**

Необходимо построить отчет по данным о поставщиках(suppliers), содержащий сводную информацию в разрезе страны и региона поставщика.   
Используйте сортировку по названию страны (N_NAME) и региона (R_NAME) на возрастание.

**Итоговый формат**

В отчете должны присутствовать следующие колонки:

|   |   |   |
|---|---|---|
|**Поле**|**Тип**|**Описание**|
|R_NAME|str|название региона|
|N_NAME|str|название страны|
|unique_supplers_count|int|число уникальных поставщиков в разрезе страны и региона|
|avg_acctbal|float|средний остаток поставщика (S_ACCTBAL)|
|mean_acctbal|float|медианный остаток поставщика (S_ACCTBAL)|
|min_acctbal|float|минимальный остаток поставщика (S_ACCTBAL)|
|max_acctbal|float|максимальный остаток поставщика (S_ACCTBAL)|

**Пример результата**

![](https://storage.yandexcloud.net/klms-public/production/learning-content/236/2398/22154/63866/396941/image.png)

**Исходные данные**

Используйте данные:

s3a://de-raw/supplier s3a://de-raw/nation s3a://de-raw/region

**Итоговое расположение и формат**

Разместите отчет в формате parquet по пути:

s3a://de-project/{USER_PATH}/suppliers_report


## **Обработка данных для отчета по Part**

**Скрипт**

[spark-job-parts.py](https://github.com/shtrausslearning/tpc-h-etl-automisation/blob/main/dags/spark-job-parts.py)

**Описание задачи**

Необходимо построить отчет по данным о грузоперевозках (part), содержащий сводную информацию в разрезе страны поставки (N_NAME), типа поставки (P_TYPE) и типа контейнера (P_CONTAINER).   
Используйте сортировку по названию страны (N_NAME) , типа поставки (P_TYPE) и типа контейнера (P_CONTAINER) на возрастание.

**Итоговый формат**

В отчете должны присутствовать следующие колонки:

|   |   |   |
|---|---|---|
|**Поле**|**Тип**|**Описание**|
|N_NAME|str|название региона|
|P_TYPE|str|название страны|
|P_CONTAINER|str|тип контейнера|
|parts_count|int|число поставок|
|avg_retailprice|float|средняя розничная цена (P_RETAILPRICE)|
|size|int|суммарный размер (P_SIZE)|
|mean_retailprice|float|медианная розничная цена(P_RETAILPRICE)|
|min_retailprice|float|минимальная розничная цена(P_RETAILPRICE)|
|max_retailprice|float|максимальная розничная цена(P_RETAILPRICE)|
|avg_supplycost|float|средняя стоимость поставки (PS_SUPPLYCOST)|
|mean_supplycost|float|медианная стоимость поставки (PS_SUPPLYCOST)|
|min_supplycost|float|минимальная стоимость поставки (PS_SUPPLYCOST)|
|max_supplycost|float|максимальная стоимость поставки (PS_SUPPLYCOST)|

**Пример результата**

![](https://storage.yandexcloud.net/klms-public/production/learning-content/236/2398/22154/63866/396942/image.png)

**Исходные данные**

Используйте данные:

s3a://de-raw/part s3a://de-raw/partsupp s3a://de-raw/supplier s3a://de-raw/nation

**Итоговое расположение и формат**

Разместите отчет в формате parquet по пути:

s3a://de-project/{USER_PATH}/parts_report
