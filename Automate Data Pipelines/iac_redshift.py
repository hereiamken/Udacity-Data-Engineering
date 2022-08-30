import boto3
import json
import configparser
import pandas as pd
import psycopg2
from time import sleep

# Read configuration parameters from file
config = configparser.ConfigParser()
config.read_file(open('airflow.cfg'))

AWS_KEY = config.get('AWS', 'KEY')
AWS_SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")

DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

REGION = config.get("DWH", "REGION")

ec2 = boto3.resource('ec2',
                     region_name="us-west-2",
                     aws_access_key_id=AWS_KEY,
                     aws_secret_access_key=AWS_SECRET
                     )

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=AWS_KEY,
                    aws_secret_access_key=AWS_SECRET
                    )

iam = boto3.client('iam', aws_access_key_id=AWS_KEY,
                   aws_secret_access_key=AWS_SECRET,
                   region_name='us-west-2'
                   )

redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET
                        )

print(AWS_KEY)


def create_role():
    try:
        print("1.1 Creating a new IAM Role")
        role = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                           )['ResponseMetadata']['HTTPStatusCode']


def creat_cluster(ROLE_ARN):
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[ROLE_ARN]
        )
    except Exception as e:
        print(e)


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_status(status):
    try:
        myClusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

        while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] != status:
            sleep(5)
            prettyRedshiftProps(myClusterProps)

        prettyRedshiftProps(myClusterProps)
    except:
        print('cluster is deleted')


def delete_cluster():
    try:
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    except:
        print('problem when deleting cluster')


def open_TCP_port(myClusterProps, DWH_PORT):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)


def test_connection(DWH_ENDPOINT, DWH_PORT):
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
            DWH_ENDPOINT, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
        cur = conn.cursor()
        cur.execute("SELECT version();")
        print('Connected successfully!')
    except Exception as e:
        print('Connected unsuccessfully!')


def main(input):
    df = pd.DataFrame({"Param":
                       ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER",
                           "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_IAM_ROLE_NAME", "REGION"],
                       "Value":
                       [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER,
                           DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_IAM_ROLE_NAME, REGION]
                       })
    print(input)

    if input == '2':
        print('Deleting cluster!')
        delete_cluster()
        check_status('deleted')
    elif input == '1':
        print('Creating cluster!')
        create_role()
        ROLE_ARN = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        print(f'ARN: {ROLE_ARN}')

        creat_cluster(ROLE_ARN)

        check_status('available')

        print(redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER))

        DWH_PORT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
            'Clusters'][0]['Endpoint']['Port']

        DWH_ENDPOINT = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
            'Clusters'][0]['Endpoint']['Address']

        test_connection(DWH_ENDPOINT=DWH_ENDPOINT, DWH_PORT=DWH_PORT)

        print(f"""Airflow AWS Connection
            Login:  {AWS_KEY}
            Password: {AWS_SECRET}
        """)

        print(f"""Airflow Redshift Connection
            Connection id:  redshift
            Connection Type:  Postgres
            Host:   {DWH_ENDPOINT}
            Schema: {DWH_DB}
            Login:  {DWH_DB_USER}
            Password: {DWH_DB_PASSWORD}
            Port: {DWH_PORT}
        """)


if __name__ == "__main__":
    input = input(
        'Click 1 to Create and 2 to Delete: ')
    main(input)
