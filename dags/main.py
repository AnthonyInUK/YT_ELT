from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
local_tz = pendulum.timezone("America/Vancouver")

default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(hours=1),
    'start_date': datetime(2025, 10, 5, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 20 * * *',  # At 20:00 every day
    catchup=False,
    tags=['youtube', 'data_pipeline'],
) as dag:
    playlistId_task = get_playlist_id()
    video_ids_task = get_video_ids(playlistId_task)
    extracted_data_task = extract_video_data(video_ids_task)
    save_to_json_task = save_to_json(extracted_data_task)

    playlistId_task >> video_ids_task >> extracted_data_task >> save_to_json_task


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into both staging and core schemas',
    schedule='0 2 * * *',  # At 20:00 every day
    catchup=False,
) as dag:
    update_staging = staging_table()
    update_core = core_table()

    update_staging >> update_core


with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in the db',
    schedule='0 17 * * *',  # At 17:00 every day
    catchup=False,
) as dag:
    soda_update_staging = yt_elt_data_quality("staging")
    soda_update_core = yt_elt_data_quality("core")

    soda_update_staging >> soda_update_core
