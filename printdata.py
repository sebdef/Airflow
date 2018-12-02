#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  1 15:07:16 2018

@author: defrens
on github
"""


from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def ot(contenu):
    file = open('/Users/defrens/Airflow/sdoutput/testfile.txt','a') 
    contenu=datetime.now().strftime("%Y-%m-%d %H:%M:%S")+':'+contenu+'\n'
    file.write(contenu) 
    file.close()
    print(contenu)
    




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['seb.defrenne@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('PRINT3', default_args=default_args, schedule_interval=timedelta(seconds=30))

# t1, t2 and t3 are examples of tasks created by instantiating operators

task = PythonOperator(
    task_id='message',
    python_callable=ot,
    op_kwargs={'contenu': 'message toutes les 30 secondes'},
    dag=dag,
)


templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""


print('ok')