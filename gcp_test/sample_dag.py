#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from airflow.models import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import timedelta, datetime
import pendulum

# time zone を日本時間にする場合、pendulum等のlibraryを利用する
local_tz = pendulum.timezone("Asia/Tokyo")

# DAGオブジェクトで利用する共通したパラメーターを定義する

default_args = {
    "owner":"Airflow",
    "start_date": datetime(2919, 1, 1, tzinfo=pendulum.timezone('Asia/Tokyo'))
    "depends_on_past":True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

#DAGオブジェクトの定義
#定期実行（schedule_interval）で定義

dag = {
    dag_id = "sample_dag",
    default_args = default_args,
    schedule_interval ='@once'
}


#個々のtask を定義する
#bash operatorを用いて現在時刻を表示する

operator_1 = BashOperator(
    task_id = 'operator_1',
    bash_command = 'echo {}'.format(datetime.now()),
    dag = dag
)

#ワークフローを見るための道具として、ダミーオペレーターを定義する
#ダミーオペレーター自身は特別な処理はない

operator_2 = DummyOperator(
    task_id = 'operator_2',
    trigger_rule = 'all_success',
    dag = dag
)

# ダミーオペレータの２つ目

operator_3 = DummyOperator(
    task_id = 'operator_3',
    trigger_rule = 'all_success',
    dag = dag
)

operator_4 = DummyOperator(
    task_id = 'operator_4',
    trigger_rule = 'all_success',
    dag = dag
)


#最後に、もう一度 bash operator を用いて現在時刻を表示させる

operator_5 = BashOperator(
    task_id = 'operator_5',
    bash_command = 'echo {}'.format(datetime.now()),
    dag = dag
)


#依存関係の記法は複数はある
#bit shift operator を使った記法で記述している
#operator_3, operator_4　について、リスト（{}）内に
#カンマ区切りで記しているこれによって、二手に分岐し、分岐後の両者が実行される

operator_1 >> operator_2 >> [operator_3, operator_4] >> operator_5