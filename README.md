# Тестовое задание на Data Engineer

**Постановка задачи:**
Требуется реализовать ELT процесс который инкрементально выгружает данные из СУБД на постоянно основе с помощью Apache Airflow. Процесс выгрузки должен проходить в 3 этапа:

1. Выгрузка данных из источника (СУБД) во временное файловое хранилище (local, s3, mini, etc) в csv формате.
2. Выгрузка данных из файлового хранилища во временную таблицу dwh (мы используем Greenplum но можно заменить на другую любую СУБД).
3. Перемещение данных c дедубликацией из временной таблицы в целевую.


**Формат данных:**

1. Таблица источника: 
CREATE TABLE order(  
  id bigint primary key,  
  student_id bigint,  
  teacher_id bigint,  
  stage varchar(10),  
  status varchar(512),  
  created_at timestamp,  
  updated_at timestamp  
);

2. Таблица назначение в dwh:  
CREATE TABLE raw_order (  
id bigint primary key,  
order_id bigint,  
student_id bigint,  
teacher_id bigint,  
stage varchar(10),  
status varchar(512),  
row_hash bigint, (hash по всем полям таблицы источника)  
created_at timestamp,  
updated_at timestamp  
);

Маппинг:  
Источник DWH  
null -> id (primary key)  
id -> order_id  
student_id -> student_id  
teacher_id -> teacher_id  
stage -> stage  
status -> status  
null -> row_hash  
created_at -> created_at  
updated_at -> updated_at  
