import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup


CITYS = ["Lviv", "Kyiv", "Zhmerynka", "Kharkiv", "Odesa"]
CITY_TO_TEAST = CITYS[0]

def _process_geo_response(ti, **kwargs):
    info = ti.xcom_pull(f"querying_geo_api.get_geo_{kwargs['city'].lower()}")[0]
    print(info["lat"], info["lon"])
    return info["lat"], info["lon"]

with DAG(dag_id="weather_dag", schedule_interval="@daily", start_date=days_ago(2), catchup=True) as dag:
    check_api = HttpSensor(
        task_id=f"get_geo_{CITY_TO_TEAST.lower()}",
        http_conn_id="weather_conn",
        endpoint="geo/1.0/direct",
        request_params={
            "appid": Variable.get("WEATHER_API_KEY"),
            "q": CITY_TO_TEAST,
            "limit": 1
        }
    )

    with TaskGroup(f"querying_geo_api") as querying_geo:
        for city in CITYS:
            extract_data = SimpleHttpOperator(
                task_id=f"get_geo_{city.lower()}",
                http_conn_id="weather_conn",
                endpoint="geo/1.0/direct",
                data={
                    "appid": Variable.get("WEATHER_API_KEY"),
                    "q": city,
                    "limit": 1
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True,
            )

    with TaskGroup("processing_tasks") as processing_tasks:
        for city in CITYS:
            process_geo_location = PythonOperator(
                task_id=f"process_geo_resp_{city.lower()}",
                python_callable=_process_geo_response,
                op_kwargs={"city": city},
                provide_context=True
            )

            query_city_weather = SimpleHttpOperator(
                task_id=f"get_weather_{city.lower()}",
                http_conn_id="weather_conn",
                endpoint="data/3.0/onecall/timemachine",
                data={
                    "appid": Variable.get("WEATHER_API_KEY"),
                    "lat":"{{ ti.xcom_pull(task_ids='processing_tasks.process_geo_resp_" + city.lower() + "')[0] }}",
                    "lon":"{{ ti.xcom_pull(task_ids='processing_tasks.process_geo_resp_" + city.lower() + "')[1] }}",
                    "dt": "{{ execution_date.int_timestamp }}"
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
                log_response=True,
            )

            sql_template = """
                INSERT INTO full_measures (timestamp, temp, city, clouds, wind_speed, humidity)
                VALUES (
                    date({{ti.xcom_pull(task_ids='processing_tasks.get_weather_CITYNAME')['data'][0]['dt']}} / 1000, 'unixepoch'),
                    {{ti.xcom_pull(task_ids='processing_tasks.get_weather_CITYNAME')['data'][0]['temp']}},
                    {{ti.xcom_pull(task_ids='processing_tasks.get_weather_CITYNAME')['data'][0]['clouds']}}, 
                    {{ti.xcom_pull(task_ids='processing_tasks.get_weather_CITYNAME')['data'][0]['wind_speed']}}, 
                    {{ti.xcom_pull(task_ids='processing_tasks.get_weather_CITYNAME')['data'][0]['humidity']}}, 
                    '_City'
                );
            """.replace("CITYNAME", city.lower()).replace('_City', city)

            inject_data = SqliteOperator(
                task_id=f"inject_data_{city.lower()}",
                sqlite_conn_id="airflow_conn",
                sql=sql_template
            )

            process_geo_location >> query_city_weather >> inject_data

    check_api >> querying_geo >> processing_tasks
