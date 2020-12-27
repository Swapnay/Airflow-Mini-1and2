from airflow.models import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import yfinance as yf
import pandas as pd
from  os.path import join


default_args1 = {
    'start_date':datetime(2020,12, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)

}
dag = DAG(
    'marketvol',
    schedule_interval='0 18 * * *',
    description='A simple DAG',
    default_args=default_args1)
data_loc ='/tmp/data/'
Variable.set("data_loc", "/tmp/data/")
Variable.set("data_to_loc","/usr/local/airflow/scripts/")
def get_full_path(current_date,ticker):
    file_path = join(data_loc,current_date )
    full_path = join(file_path,ticker)
    return full_path

def custom_query(**kwargs):
    aapl1 = pd.read_csv(get_full_path(kwargs['ds'],kwargs['ticker1']+".csv"))
    print('AAPL')
    print(aapl1.head())
    tsla = pd.read_csv(get_full_path(kwargs['ds'],kwargs['ticker2']+".csv"))
    print('TSLA')
    print(tsla.head())

def download_stocks(**kwargs):

    prev_date = kwargs['prev_ds']
    cur_date =datetime.today()
    ticker = kwargs['ticker']
    tsla_df = yf.download(ticker, start=prev_date, end=cur_date, interval='1m')
    full_path = get_full_path(kwargs['ds'],kwargs['ticker']+".csv")
    tsla_df.to_csv(full_path,header=False)

folder = BashOperator(
    task_id='create_folder',
    bash_command='mkdir -p {{var.value.data_loc}}{{ ds }}',
    dag=dag)

load_hdfs_AAPL = BashOperator(
    task_id='load_AAPL_hdfs',
    bash_command='cp {{ var.value.data_loc }}{{ ds }}/AAPL.csv {{var.value.data_to_loc}}/{{ ds }}AAPL.csv',
    dag=dag)

load_hdfs_TSLA = BashOperator(
    task_id='load_TSLA_hdfs',
    bash_command='cp {{var.value.data_loc}}{{ ds }}/TSLA.csv {{var.value.data_to_loc}}/{{ ds }}TSLA.csv',
    dag=dag)

AAPL_task = PythonOperator(task_id='AAPL_task',
                             python_callable=download_stocks,
                             op_kwargs={'ticker':'AAPL'},
                             provide_context=True,
                             dag=dag)

TSLA_task = PythonOperator(task_id='TSLA_task',
                           python_callable=download_stocks,
                           op_kwargs={'ticker':'TSLA'},
                           provide_context=True,
                           dag=dag)

query_task = PythonOperator(task_id='query_task',
                           python_callable=custom_query,
                           op_kwargs={'ticker1':'AAPL','ticker2':'TSLA'},
                           provide_context=True,
                           dag=dag)

folder>>AAPL_task>>load_hdfs_AAPL
folder>>TSLA_task>>load_hdfs_TSLA
load_hdfs_AAPL>>query_task<<load_hdfs_TSLA
