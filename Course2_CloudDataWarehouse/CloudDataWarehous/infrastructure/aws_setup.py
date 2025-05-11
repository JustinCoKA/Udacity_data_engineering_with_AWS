import boto3
import json
import configparser
import os
import time
from botocore.exceptions import ClientError

config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'dwh.cfg')
config = configparser.ConfigParser()
config.read(config_path)

def create_iam_role():

    iam = boto3.client('iam')
    role_name = config.get('IAM_ROLE', 'IAM_ROLE_NAME')

    # Detach policy
    try:
        print(f"Detaching policies from IAM Role '{role_name}' if any...")
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
        )
    except ClientError as e:
        if e.response['Error']['Code'] != 'NoSuchEntity':
            print(f'Warning Failed to detach policy: {e}')

    # delete role
    try:
        print(f"Deleting IAM Role '{role_name}' if it exists...")
        iam.delete_role(RoleName=role_name)
    except iam.exceptions.NoSuchEntityException:
        pass
    except ClientError as e:
        print(f"Warning: Could not delete existing role: {e}")

    # create role
    print("Creating IAM Role...")

    iam.create_role(
        Path='/',
        RoleName=role_name,
        Description='Allows Redshift clusters to call AWS services.',
        AssumeRolePolicyDocument=json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'},
                'Action': 'sts:AssumeRole'
            }]
        })
    )

    # Attach Policy
    print("Attaching AmazonS3ReadOnlyAccess policy...")
    response = iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )
    print("Attach policy Status: ", response['ResponseMetadata']['HTTPStatusCode'])

    # Save Role ARN to config
    role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
    print("Role ARN", role_arn)

    config.set('IAM_ROLE', 'ARN', role_arn)
    with open(config_path, 'w') as cfg_file:
        config.write(cfg_file)

def create_redshift_cluster():
    redshift = boto3.client('redshift', region_name=config.get('CLUSTER', 'REGION'))

    try:
        redshift.create_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            NodeType=config.get('CLUSTER', 'NODE_TYPE'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            DBName=config.get('CLUSTER', 'DB_NAME'),
            ClusterType='single-node',
            IamRoles=[config.get('IAM_ROLE', 'ARN')],
            PubliclyAccessible=True
        )
        print("Redshift cluster creation initiated.")

    except Exception as e:
        print(e)

def check_cluster_status():
    redshift = boto3.client('redshift', region_name=config.get('CLUSTER', 'REGION'))

    try:
        cluster_info = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))
        status = cluster_info['Clusters'][0]['ClusterStatus']
        endpoint = cluster_info['Clusters'][0]['Endpoint']['Address']
        role_arn = cluster_info['Clusters'][0]['IamRoles'][0]['IamRoleArn']

        print(f"Cluster status: {status}")
        print(f"Cluster endpoint: {endpoint}")
        print(f"Cluster role arn: {role_arn}")

        config.set('CLUSTER', 'HOST', endpoint)
        config.set('IAM_ROLE', 'ARN', role_arn)
        with open(config_path, 'w') as cfg_file:
            config.write(cfg_file)

    except ClientError as e:
        print("Error checking cluster status:", e.response['Error']['Message'])

def delete_redshift_cluster():
    redshift = boto3.client('redshift', region_name=config.get('CLUSTER', 'REGION'))
    cluster_id = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    try:
        print(f"Deleting cluster {cluster_id}")
        response = redshift.delete_cluster(ClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
        print(f"Deleted cluster {cluster_id}")
        print(f"Cluster status: {response['Cluster']['ClusterStatus']}")

    except ClientError as e:
        print("Error deleting cluster status:", e.response['Error']['Message'])

def wait_for_cluster_available(poll_interval=30, timeout_seconds=600):
    """
    Wait until the Redshift cluster becomes 'available'.
    Polls every `poll_interval` seconds up to `timeout_seconds`.
    """
    redshift = boto3.client('redshift', region_name=config.get('CLUSTER', 'REGION'))
    cluster_id = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
    elapsed = 0

    print(f"🔄 Waiting for Redshift cluster '{cluster_id}' to become available...")

    while elapsed < timeout_seconds:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=cluster_id)
            cluster = response['Clusters'][0]
            status = cluster['ClusterStatus']
            print(f"⏳ Status: {status} (checked at {elapsed} seconds)")

            if status == 'available':
                endpoint = cluster['Endpoint']['Address']
                role_arn = cluster['IamRoles'][0]['IamRoleArn']
                print(f"✅ Cluster is available!")
                print(f"🔗 Endpoint: {endpoint}")
                print(f"🛡️ IAM Role ARN: {role_arn}")

                config.set('CLUSTER', 'HOST', endpoint)
                config.set('IAM_ROLE', 'ARN', role_arn)
                with open(config_path, 'w') as cfg_file:
                    config.write(cfg_file)

                return True

        except ClientError as e:
            print("⚠️ Error while checking status:", e.response['Error']['Message'])
            break

        time.sleep(poll_interval)
        elapsed += poll_interval

    print("❌ Timeout: Redshift cluster did not become available in time.")
    return False

if __name__ == '__main__':
    create_iam_role()
    create_redshift_cluster()
    check_cluster_status()
    delete_redshift_cluster()
    wait_for_cluster_available()