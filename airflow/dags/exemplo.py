from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from datetime import datetime
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secret_access_key')

client = boto3.client(
    'emr', region_name='us-east-1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

default_args = {
    'owner': 'Marcos Vieira',
    'start_date': datetime(2022, 11, 11)
}

@dag(
    default_args=default_args,
    schedule_interval="@once",
    description="Executa um job Spark no EMR",
    catchup=False,
    tags=['Spark','EMR'])
def indicadores_titanic():

    inicio = DummyOperator(task_id='inicio')

    @task
    def tarefa_inicial():
        print("Começou!!")

    @task
    def emr_create_cluster():
        cluster_id = client.run_job_flow( # Cria um cluster EMR
            Name='MV_EMR_PUCMINAS',
            ServiceRole='EMR_DefaultRole',
            JobFlowRole='EMR_EC2_DefaultRole',
            VisibleToAllUsers=True,
            LogUri='s3://aws-logs-936492601478/elasticmapreduce/',
            ReleaseLabel='emr-6.8.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'Master nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker nodes',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'MVV_PUC_mykeypair',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-0f4f708339977a469'
            },

            Applications=[{'Name': 'Spark'}, {'Name': 'Hive'}],
        )
        return cluster_id["JobFlowId"]


    @task
    def wait_emr_cluster(cid: str):
        waiter = client.get_waiter('cluster_running')

        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 60
            }
        )
        return True


    
    @task
    def emr_process_titanic(cid: str):
        newstep = client.add_job_flow_steps(
            JobFlowId=cid,
            Steps=[
                {
                    'Name': 'Processa indicadores Titanic',
                    'ActionOnFailure': "CONTINUE",
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                '--packages', 'io.delta:delta-core_2.12:2.1.0',
                                's3://puc-pre-orq-flu-dad-marcos-vieira/pyspark/titanic_example_delta.py'
                                ]
                    }
                }
            ]
        )
        return newstep['StepIds'][0]

    @task
    def wait_emr_job(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')

        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 10,
                'MaxAttempts': 600
            }
        )
    
    @task
    def terminate_emr_cluster(cid: str):
        res = client.terminate_job_flows(
            JobFlowIds=[cid]
        )

    fim = DummyOperator(task_id="fim")

    # Orquestração
    tarefainicial = tarefa_inicial()
    cluster = emr_create_cluster()
    inicio >> tarefainicial >> cluster

    esperacluster = wait_emr_cluster(cluster)

    indicadores = emr_process_titanic(cluster) 
    esperacluster >> indicadores

    wait_step = wait_emr_job(cluster, indicadores)

    terminacluster = terminate_emr_cluster(cluster)
    wait_step >> terminacluster >> fim
    #---------------

execucao = indicadores_titanic()