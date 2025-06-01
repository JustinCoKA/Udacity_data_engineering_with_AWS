import boto3
import json
import configparser
import os
import time
from botocore.exceptions import ClientError
from boto3.session import Session

config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'dwh.cfg')
config = configparser.ConfigParser()
config.read(config_path)

aws_session = Session() # This creates a session using your default AWS CLI config

def create_iam_role():

    iam = aws_session.client('iam') # aws_session이 정의되어 있어야 함
    role_name = config.get('IAM_ROLE', 'IAM_ROLE_NAME')
    s3_read_policy_arn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'

    print(f"--- Managing IAM Role: {role_name} ---")

    # Detach policy
    try:
        print(f"Attempting to detach policy '{s3_read_policy_arn}' from role '{role_name}'...")
        iam.detach_role_policy(RoleName=role_name, PolicyArn=s3_read_policy_arn)
        print(f"Successfully detached policy from role '{role_name}'.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"Role '{role_name}' or policy not found for detachment (this is okay).")
        elif e.response['Error']['Code'] == 'DeleteConflict':
            print(
                f"Warning: Cannot detach policy, it might be used by other entities or role deletion is pending: {e.response['Error']['Message']}")
        else:
            print(
                f"Warning: Failed to detach policy from role '{role_name}': {e.response['Error']['Code']} - {e.response['Error']['Message']}")

    # delete role
    try:
        print(f"Attempting to delete IAM Role '{role_name}'...")
        iam.delete_role(RoleName=role_name)
        print(f"Successfully initiated deletion for role '{role_name}'. Waiting for it to complete...")
        time.sleep(10)

        print(f"Assumed role '{role_name}' is deleted.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print(f"Role '{role_name}' not found for deletion (this is okay).")
        else:
            print(
                f"Warning: Could not delete existing role '{role_name}': {e.response['Error']['Code']} - {e.response['Error']['Message']}")

    # create role
    try:
        print(f"Attempting to create IAM Role: {role_name}...")
        assume_role_policy_document = json.dumps({
            'Version': '2012-10-17',
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'},
                'Action': 'sts:AssumeRole'
            }]
        })
        role_response = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description='Allows Redshift clusters to call AWS services.',
            AssumeRolePolicyDocument=assume_role_policy_document
        )
        created_role_arn = role_response['Role']['Arn']
        print(f"Successfully created role '{role_name}' with ARN: {created_role_arn}. Waiting for propagation...")
        time.sleep(15)

    # Attach Policy
        time.sleep(5)
        print(f"Verifying role '{role_name}' and getting its ARN...")
        role_details = iam.get_role(RoleName=role_name)
        final_role_arn = role_details['Role']['Arn']
        print(f"Role ARN verified: {final_role_arn}")

        config.set('IAM_ROLE', 'ARN', final_role_arn)
        with open(config_path, 'w') as cfg_file:
            config.write(cfg_file)
        print(f"Successfully saved Role ARN '{final_role_arn}' to dwh.cfg.")
        print(f"--- IAM Role '{role_name}' setup complete. ---")

    except ClientError as e:
        print(f"ERROR during IAM role creation/management for '{role_name}':")
        print(f"  Error Code: {e.response['Error']['Code']}")
        print(f"  Error Message: {e.response['Error']['Message']}")
        print("  Please check IAM user permissions and role status in AWS console.")

    except Exception as e_general:
        print(f"An unexpected error occurred in create_iam_role: {e_general}")


def create_redshift_cluster():
    redshift = aws_session.client('redshift', region_name=config.get('CLUSTER', 'REGION'))

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
    redshift = aws_session.client('redshift', region_name=config.get('CLUSTER', 'REGION'))

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
    # Use the correctly initialized 'aws_session' and specify region
    redshift = aws_session.client('redshift', region_name=config.get('CLUSTER', 'REGION'))
    cluster_id = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')

    try:
        print(f"Deleting cluster {cluster_id}")
        response = redshift.delete_cluster(ClusterIdentifier=cluster_id, SkipFinalClusterSnapshot=True)
        print(f"Deleted cluster {cluster_id}")
        print(f"Cluster status: {response['Cluster']['ClusterStatus']}")

    except ClientError as e:
        print("Error deleting cluster status:", e.response['Error']['Message'])

def wait_for_cluster_available(poll_interval=30, timeout_seconds=600):
    """
    Wait until the Redshift cluster becomes 'available'.
    Polls every `poll_interval` seconds up to `timeout_seconds`.
    """
    redshift = aws_session.client('redshift', region_name=config.get('CLUSTER', 'REGION'))
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