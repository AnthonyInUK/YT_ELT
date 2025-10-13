import pytest
import requests

def test_youtube_api_response(airflow_variables):
    api_key = airflow_variables("API_KEY")
    channel_handle = airflow_variables("CHANNEL_HANDLE")
    
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"

    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail("Failed to get YouTube API failed: {e}")


# to test if the airflow connection to the postgres database is working
def test_postgres_connection(real_postgres_connections):
    cursor = None
    try:
        cursor = real_postgres_connections.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1
    except psycopg2.Error as e:
        pytest.fail("Failed to connect to PostgreSQL: {e}")
    finally:
        if cursor is not None:
            cursor.close()





