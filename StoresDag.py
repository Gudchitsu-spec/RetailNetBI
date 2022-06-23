from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator


args = {
  'owner': 'airflow',
  'retries': 5,
  "task_concurency": 1 
  }

with DAG (
  dag_id = "Techcop_stores_datadelivery",
  default_args= args,
  schedule_interval= '10 5 * * *',
  start_date =datetime (2022,6,1),  
  catchup = False 
  )as dag:

  KrasnogorskTask = BashOperator (
    task_id='Krasnogorsk_task', 
    bash_command=f'U:/RetailNet/Scripts/Kransogorsk_etl.py', 
    dag = dag)


  OdincovoTask = BashOperator (
    task_id='Odincovo_task', 
    bash_command=f'U:/RetailNet/Scripts/Odincovo_etl.py', 
    dag = dag)


  OpalichaTask = BashOperator (
    task_id='Opalicha_Task', 
    bash_command=f'U:/RetailNet/Scripts/Opalicha_etl.py', 
    dag = dag)
  
  DedovskTask = BashOperator (
    task_id='Dedovsk_Task', 
    bash_command=f'U:/RetailNet/Scripts/Dedovsk_etl.py', 
    dag = dag)
  
  BalashihaTask = BashOperator (
    task_id='Balashiha_Task', 
    bash_command=f'U:/RetailNet/Scripts/Balashiha_etl.py', 
    dag = dag)
  
  PavshinoTask = BashOperator (
    task_id='Pavshino_Task', 
    bash_command=f'U:/RetailNet/Scripts/Pavshino_etl.py', 
    dag = dag)
  
  NullCleaner = BashOperator (
    task_id='Null_Cleaner', 
    bash_command=f'U:/RetailNet/Scripts/NullCleaner.py', 
    dag = dag)
  
  [KrasnogorskTask, OdincovoTask, OpalichaTask, DedovskTask, BalashihaTask, PavshinoTask] >> NullCleaner

    
  

