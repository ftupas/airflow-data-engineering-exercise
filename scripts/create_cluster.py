import boto3
import configparser
import json
import time
from process_config import *

def create_aws_client(service):
    """
    Creates an AWS client
    
    Params:
        service(str): Name of the AWS service
    """
    print(f"CREATING {service} client.")
    return boto3.client(service,
                        region_name = REGION,
                        aws_access_key_id = KEY,
                        aws_secret_access_key = SECRET
                        )

def create_aws_resource_client(service):
    """
    Creates an AWS client for a resource
    
    Params:
        service(str): Name of the AWS service
    """
    print(f"CREATING {service} client.")
    return boto3.resource(service,
                          region_name = REGION,
                          aws_access_key_id = KEY,
                          aws_secret_access_key = SECRET
                          )

def create_iam_role(iam):
    """
    Create an AWS IAM Role

    Params:
        iam(object): IAM client that was created with AWS Keys and Secrets
    """
    try:
        print("CREATING a new IAM Role.")
        iam.create_role(
            Path = "/",
            RoleName = DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument = json.dumps(
                {"Statement": [{"Action": "sts:AssumeRole",
                                "Effect": "Allow",
                                "Principal": {"Service": "redshift.amazonaws.com"}}],
                 "Version": "2012-10-17"}
            )
        )
    except Exception as e:
        print(e)
        iam.get_role(RoleName = DWH_IAM_ROLE_NAME)    

def attach_role_policy_iam(iam, policy):
    """
    Attaching policy to an AWS IAM Role

    Params:
        iam(object): IAM client that was created with AWS Keys and Secrets
        policy(str): AWS Policy that specifies access
    """
    print(f"ATTACHING policy to {DWH_IAM_ROLE_NAME}.")
    
    try:
        iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                                    PolicyArn = policy
                                    )
    except Exception as e:
        print(e)

def get_iam_role_arn(iam):
    """
    Gets the Amazon Resource Name of the IAM Role

    Params:
        iam(object): IAM client that was created with AWS Keys and Secrets
    """
    print(f"GETTING IAM role ARN of {DWH_IAM_ROLE_NAME}.")
    try:
        roleArn = iam.get_role(RoleName = DWH_IAM_ROLE_NAME)["Role"]["Arn"]
    except Exception as e:
        print(e)
    return(roleArn)

def create_redshift_cluster(redshift, roleArn):
    """
    Create a Redshift cluster

    Params:
        redshift(object): Redshift client that was created with AWS Keys and Secrets
        roleArn(str): ARN (Amazon Resource Name) of IAM Role
    """
    print("CREATING Redshift cluster.")
    try:
        redshift.create_cluster(
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]     
        )
    except Exception as e:
        print(e)

def get_cluster_info(redshift):
    """
    Gets the information on the Redshift cluster

    Params:
        redshift(object): Redshift client that was created with AWS Keys and Secrets
    """
    response = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)["Clusters"][0]
    return response

def set_cluster_access(ec2, response):
    """
    Open incoming TCP port access to cluster endpoint

    Params:
        ec2(object): EC2 client that was created with AWS Keys and Secrets
        response(dict): Dictionary of information on the cluster
    """
    print("SETTING access to cluster endpoint.")
    try:
        vpc = ec2.Vpc(id = response["VpcId"])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp=IP,
            IpProtocol="TCP",
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

def write_cluster_info(file, redshift, response):
    """
    Record the Endpoint and IAM ARN of the redshift cluster into the config file

    Params:
        file(str): Name of the config file
        redshift(object): Redshift client that was created with AWS Keys and Secrets
        response(dict): Dictionary of information on the cluster
    """
    print(f"WRITING cluster Endpoint and IAM Role ARN to {file}.")

    config = configparser.ConfigParser()

    with open(file, "r") as f:
        config.read_file(f)

        config.set("CLUSTER", "HOST", response["Endpoint"]["Address"])
        config.set("IAM_ROLE", "ARN", response["IamRoles"][0]["IamRoleArn"])

    with open(file, "w+") as w:
        config.write(w)

def main():
    """
    Function used to create a Redshift cluster and necessary policies and roles to access S3 buckets.
    Usage: python.exe create_cluster.py Windows
           python     create_cluster.py Linux/Mac 
    """
    # Create IAM, Redshift, and EC2 clients
    iam = create_aws_client("iam")
    redshift = create_aws_client("redshift")
    ec2 = create_aws_resource_client("ec2")

    # Create IAM Role that makes Redshift able to access (ReadOnly) S3 bucket
    create_iam_role(iam)

    # Attach policy to dwhRole
    attach_role_policy_iam(iam, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")

    # Get IAM role ARN
    roleArn = get_iam_role_arn(iam)
    
    # Create Redshift Cluster
    create_redshift_cluster(redshift, roleArn)

    # Prompt user when cluster is created
    while True:
        time.sleep(30)
        response = get_cluster_info(redshift)
        status = response["ClusterStatus"]

        if status.lower() == "available":
            print("Cluster created.")
            break
        else:
            print("CREATING...")
    
    # Open incoming TCP port to access cluster endpoint
    response = get_cluster_info(redshift)
    set_cluster_access(ec2, response)

    # Write cluster info into config file
    write_cluster_info("dwh.cfg", redshift, response)

    print("DONE!")
        
if __name__ == "__main__":
    main()