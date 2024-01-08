from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime


def flatten_json(json_obj, parent_key='', sep='_'):
    flattened = {}
    for key, value in json_obj.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key, sep=sep))
        else:
            flattened[new_key] = value
    return flattened

def flatten_json_list(json_list, sep='_'):
    flattened_list = []
    for json_obj in json_list:
        flattened_list.append(flatten_json(json_obj, sep=sep))
    return flattened_list

def query_data(to_query):
    from pymongo import MongoClient
    import pandas as pd
    from datetime import datetime, timezone

    client = MongoClient("cluster_URL")

    db = client.company
    departments = db.department

    data = departments.find({'name': to_query + " department"}, {'_id': 0, 'employees': 1})
    for x in data:
        d = x
    f = flatten_json_list(d['employees'])
    
    df = pd.DataFrame(f)
    df['startdate'] = pd.to_datetime(df['startdate']).dt.to_period('M')
    df['birthdate'] = pd.to_datetime(df['birthdate'])
    df['current_date'] = pd.to_datetime(datetime.now(timezone.utc))
    df['age'] = (df['current_date']-df['birthdate']).dt.days // 365
    df = df.drop(columns=['current_date', 'birthdate'])
    df.to_excel('/opt/airflow/dags/data/'+to_query+'report.xlsx', index=False)

body = """
    Hello,<br>

    Please find the attached file.<br>

    Best regards,<br>
    Anas Jouani
    """

default_arguments = {
    'owner': 'Anas',
    'start_date': datetime(2023, 12, 12),
    'schedule_interval':"@weekly",
    'tags':["IDSCC"],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(dag_id="project_dag", default_args=default_arguments) as dag:

    fetching_IT_data = PythonOperator(
        task_id="fetch_IT_data",
        python_callable=query_data,
        op_kwargs={'to_query': "IT"},
    )

    fetching_HR_data = PythonOperator(
        task_id="fetch_HR_data",
        python_callable=query_data,
        op_kwargs={'to_query': "HR"},
    )

    sending_email1 = EmailOperator(
        task_id='send_email1',
        to="anas.jouani20@ump.ac.ma",
        subject="IT department data",
        html_content=body,
        files=['/opt/airflow/dags/data/ITreport.xlsx'],
    )

    sending_email2 = EmailOperator(
        task_id='send_email2',
        to="anas.jouani20@ump.ac.ma",
        subject="HR department data",
        html_content=body,
        files=['/opt/airflow/dags/data/HRreport.xlsx'],
    )

    fetching_IT_data >> sending_email1
    fetching_HR_data >> sending_email2
