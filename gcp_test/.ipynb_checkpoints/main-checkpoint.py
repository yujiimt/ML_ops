import os
from google.cloud import bigquery


#GCPのプロジェクトIDを指定
PROJECT_ID = os.getenv("GCP_PROJECT")

#読み込み先のBQのデータセット名を指定
BQ_DATASET = "[データセット名]"

#読み込み先のBQのテーブル名を指定
BQ_TABLE = "[テーブル名]"



def bq_load_from_gcs(evenet, context):
    #BQのクライアントを作成
    client = bigquery.Client()
    #データセットとテーブルを指定
    tabel_ref = client.dataset(BQ_DATASET).table(BQ_TABLE)
    
    
    #読み取りジョブの設定
    job_config = big.LoadJobConfig()
    #スキーマの自動検出を有効にしたい場合はTrueを指定する
    #job_config.autodetect = True
    #既存のデータは上書きする
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    #フォーマットはcsv(デフォルト)を指定
    job_config.source_format = bigqury.SourceFormat.CSV
    #最初の１行はヘッダ行として読み飛ばし
    job_config.skip_leading_rows = 1
    
    
    #CSVデータの読み込み元バケットを指定
    uri = 'gs://' + event['bucket'] + '/' +event['name']
    
    #読み込みジジョブの実行
    load_job = client.load_table_uri(
        uri,
        table_ref,
        job_config = job_confing
    )
    
    print(f"Starting job {load_job.job_id}")