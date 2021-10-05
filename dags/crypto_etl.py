import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.hooks.postgres_hook import PostgresHook
import os
import ccxt
import matplotlib.pyplot as plt
import pandas as pd
from fpdf import FPDF

today = now = datetime.now()


def create_pdf(data, percent_change, file_name):
    df = pd.read_json(json.dumps(data))
    df = df.head(50)
    fig, ax = plt.subplots()
    ax.set_ylabel("Price", color="red")
    plt.xticks(rotation=60)
    ax.plot(df['ts'], df['close'])
    ax.axes.xaxis.set_visible(False)
    plt.savefig(file_name)
    abs_path_binance = os.path.abspath(file_name)
    # print('File ABS PATH ' + abs_path_binance)

    # Create PDF
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font('Arial', 'B', 36)
    pdf.text(75, 60, "BTC Analysis")
    pdf.set_font('Arial', 'U', 14)
    pdf.text(50, 68, "Hourly Analysis of BTC data on Binance, FTX and ByBit")
    pdf.add_page()
    pdf.set_font('Arial', 'B', 14)
    pdf.text(20, 20, "Binance")
    pdf.set_xy(20, 30)
    pdf.image(abs_path_binance, w=100, h=50)
    pdf.set_font('Arial', 'B', 12)
    pdf.text(20, 80, "Change in Volume in %:- {}".format(round(percent_change)))
    pdf.set_author('Adnan Siddiqi')
    pdf.output('btc_analysis.pdf', 'F')
    abs_path_binance_pdf = os.path.abspath('btc_analysis.pdf')
    return abs_path_binance_pdf


def gen_ts_text(ts):
    timestamp = ts / 1000
    dt_object = datetime.fromtimestamp(timestamp)
    # print(dt_object.strftime('%Y-%m-%d %H:%M:%S'))
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')


def fetch_binance_ohlcv(**kwargs):
    exchange = ccxt.binance()
    ohlcv = exchange.fetch_ohlcv('BTC/USDT', timeframe='1h', limit=100)
    return ohlcv


def fetch_ftx_ohlcv(**kwargs):
    exchange = ccxt.ftx()
    ohlcv = exchange.fetch_ohlcv('BTC/USDT', timeframe='1h', limit=100)
    return ohlcv


def fetch_bybit_ohlcv(**kwargs):
    exchange = ccxt.bybit()
    ohlcv = exchange.fetch_ohlcv('BTC/USDT', timeframe='1h', limit=100)
    return ohlcv


def transform_data(**kwargs):
    ti = kwargs['ti']
    binance = []
    ftx = []
    bybit = []
    binance_ohlcv_data, ftx_ohlcv_data, fetch_bybit_ohlcv = ti.xcom_pull(key=None, task_ids=['fetch_binance_ohlcv',
                                                                                             'fetch_ftx_ohlcv',
                                                                                             'fetch_bybit_ohlcv'])
    for record in binance_ohlcv_data:
        ts = gen_ts_text(record[0])
        binance.append({'ts': ts, 'close': record[4], 'volume': record[5]})

    for record in ftx_ohlcv_data:
        ts = gen_ts_text(record[0])
        ftx.append({'ts': ts, 'close': record[4], 'volume': record[5]})

    for record in fetch_bybit_ohlcv:
        ts = gen_ts_text(record[0])
        bybit.append({'ts': ts, 'close': record[4], 'volume': record[5]})

    return {'binance': binance, 'ftx': ftx, 'bybit': bybit}


def create_text_file(**kwargs):
    a = []

    today = datetime.now()
    time_part = today.strftime('%d%m%y%H%m%S')

    ti = kwargs['ti']
    close_data = ti.xcom_pull(key=None, task_ids=['transform_data'])
    binance_data = close_data[0]['binance']
    ftx_data = close_data[0]['ftx']
    bybit_data = close_data[0]['bybit']

    # Creation of Text files
    file_binance = 'binance_data_{}.txt'.format(time_part)
    file_ftx = 'ftx_data_{}.txt'.format(time_part)
    file_bybit = 'bybit_data_{}.txt'.format(time_part)
    abs_path_binance = os.path.abspath(file_binance)
    abs_path_ftx = os.path.abspath(file_ftx)
    abs_path_bybit = os.path.abspath(file_bybit)

    with open(file_binance, 'w', encoding='utf8') as f:
        f.write(json.dumps(binance_data))

    with open(file_ftx, 'w', encoding='utf8') as f:
        f.write(json.dumps(ftx_data))

    with open(file_bybit, 'w', encoding='utf8') as f:
        f.write(json.dumps(bybit_data))

    a.append(abs_path_binance)
    a.append(abs_path_ftx)
    a.append(abs_path_bybit)
    # Sending file name for the next step
    return ','.join(a)


# To FTP files to destination
def ftp_file(**kwargs):
    ti = kwargs['ti']
    files = ti.xcom_pull(key=None, task_ids=['create_text_file'])
    all_files = files[0].split(',')
    hook = FTPHook('MYFTP')

    for file in all_files:
        uploaded_file = file
        binance_file_path = uploaded_file.split('/')
        uploaded_file_name = binance_file_path[len(binance_file_path) - 1]
        hook.store_file(uploaded_file_name, uploaded_file)
    hook.close_conn()
    return True


# To FTP files to destination
def ftp_pdf_file(**kwargs):
    ti = kwargs['ti']
    today = datetime.now()
    time_part = today.strftime('%d%m%y%H%m%S')
    file_name = ti.xcom_pull(key=None, task_ids=['generate_pdf_reports'])
    hook = FTPHook('MYFTP')

    uploaded_file = file_name[0]
    print('PATH OF FILE = ' + uploaded_file)

    binance_pdf_path = uploaded_file.split('/')
    uploaded_file_name = binance_pdf_path[len(binance_pdf_path) - 1]
    uploaded_file_name = 'BTC_ANALYTICS_{}.pdf'.format(time_part)
    hook.store_file(uploaded_file_name, uploaded_file)
    hook.close_conn()
    return True


def load_data(**kwargs):
    ti = kwargs['ti']
    close_data = ti.xcom_pull(key=None, task_ids=['transform_data'])
    binance_data = close_data[0]['binance']
    ftx_data = close_data[0]['ftx']
    bybit_data = close_data[0]['bybit']
    hook = PostgresHook('Crypto_DB')
    connection = hook.get_conn()
    cursor = connection.cursor()
    for record in binance_data:
        sql = "INSERT INTO close_binance(close,ts) VALUES('{}','{}')".format(record['close'], record['ts'])
        cursor.execute(sql)
        connection.commit()

    for record in ftx_data:
        sql = "INSERT INTO close_ftx(close,ts) VALUES('{}','{}')".format(record['close'], record['ts'])
        cursor.execute(sql)
        connection.commit()

    for record in bybit_data:
        sql = "INSERT INTO close_bybit(close,ts) VALUES('{}','{}')".format(record['close'], record['ts'])
        cursor.execute(sql)
        connection.commit()

    return {'binance': binance_data, 'ftx': ftx_data, 'bybit': bybit_data}


def calculate_percentage(initial_value, final_value):
    change = (final_value - initial_value) / initial_value
    change_percent = change * 100
    return change_percent


def generate_pdf_reports(**kwargs):
    ti = kwargs['ti']
    close_data = ti.xcom_pull(key=None, task_ids=['load_data'])
    binance_data = close_data[0]['binance']
    ftx_data = close_data[0]['ftx']
    bybit_data = close_data[0]['bybit']

    initial_volume_record_binance = binance_data[0]
    final_volume_record_binance = binance_data[len(binance_data) - 1]

    binance_change_percent = calculate_percentage(initial_volume_record_binance['volume'],
                                                  final_volume_record_binance['volume']
                                                  )

    pdf_path = create_pdf(binance_data, binance_change_percent, 'binance_graph.png')
    return pdf_path


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 10, 4, 11, 00, 00),
    'concurrency': 1,
    'retries': 0
}

with DAG('crypto_analysis',
         catchup=False,
         default_args=default_args,
         schedule_interval='*/2 * * * *',
         # schedule_interval=None,
         ) as dag:
    fetch_binance_ohlcv = PythonOperator(task_id='fetch_binance_ohlcv',
                                         python_callable=fetch_binance_ohlcv)

    fetch_ftx_ohlcv = PythonOperator(task_id='fetch_ftx_ohlcv',
                                     python_callable=fetch_ftx_ohlcv)

    fetch_bybit_ohlcv = PythonOperator(task_id='fetch_bybit_ohlcv',
                                       python_callable=fetch_bybit_ohlcv)

    transform_data = PythonOperator(task_id='transform_data',
                                    python_callable=transform_data)

    create_text_file = PythonOperator(task_id='create_text_file',
                                      python_callable=create_text_file)

    load_data = PythonOperator(task_id='load_data',
                               python_callable=load_data)

    generate_pdf_reports = PythonOperator(task_id='generate_pdf_reports',
                                          python_callable=generate_pdf_reports)

    ftp_file = PythonOperator(task_id='ftp_file',
                              python_callable=ftp_file)

    ftp_pdf_file = PythonOperator(task_id='ftp_pdf_file',
                                  python_callable=ftp_pdf_file)

[fetch_binance_ohlcv, fetch_bybit_ohlcv, fetch_ftx_ohlcv] >> transform_data >> [
    ftp_pdf_file << generate_pdf_reports << load_data,
    ftp_file << create_text_file]
