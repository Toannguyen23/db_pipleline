from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
default_args = {
    "onwer": "minh toan",
    "start_date": days_ago(0)
}

def get_data():
    import requests
    import json
    
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} {location['city']}, "\
                      f"{location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    
    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    current_time = time.time()

    producer = KafkaProducer(
        bootstrap_servers = ['broker:29092'],
        max_block_ms = 5000, 
    )
    while True:
        if time.time()  > current_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f"Co loi xay ra: {e}")
            continue
    
with DAG(
    'thu_thap_tu_dong',
    default_args= default_args,
    schedule_interval= '@daily',
    catchup= False
) as dag:

    streaming_task = PythonOperator(
    task_id ="tream_data_from_api",
    python_callable= stream_data
)
    
     
# stream_data() 