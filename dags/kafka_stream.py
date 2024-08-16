from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid


default_args = {
    'owner': 'tridoan',
    'start_date': datetime(2024, 8, 12, 10, 00)
}



    
    
def get_data():
    import requests
    
    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res
        
def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {str(location['street']['name'])}, "\
        f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['phone']=res['phone']
    data['registered_date'] = res['registered']['date']
    data['picture'] = res['picture']['medium']
    data['id']= str(uuid.uuid4())
    
    return data
    
def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging
    
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_tiem= time.time()
    print("Sending data to kafka...")
    while True:
        if time.time() > curr_tiem +360:
            break
        try:
            res = get_data()
            res = format_data(res)
            
            producer.send('users_created', json.dumps(res).encode('utf8'))
        except Exception as e:
            logging.error(f"An error occured: {e}")
            continue

with DAG(
    'user_automation', 
    default_args=default_args,
    # schedule='@daily',
    catchup=False
) as dag:
    stream_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

