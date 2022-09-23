import pandas as pd
import boto3
import json
import configparser
import time

# Neccessary steps before executing:
## - Create a new IAM user in your AWS account
## - AWS user should get AdministratorAccess from the Attach existing policies directly tab
## - Take note of the access key and the secret key
## - Add those in the file 'config_redshift.cfg' at [AWS] KEY, SECRET


def read_redshift_config():
    '''
    Reads the config from config_redshift.cfg file and returns all parameters
    '''
    config = configparser.ConfigParser()
    config.read_file(open('config_redshift.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


    config_df = pd.DataFrame({"Param":
                      ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                  "Value":
                      [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
                 })
    print("These are the cluster configurations: ")
    print(config_df)
    return(KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME)
    
    
def create_clients(KEY, SECRET):
    '''
    Created clients for ec2, s3, iam and redshift and returns the client objects
    '''
    ec2 = boto3.resource('ec2',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )
    print("Created ec2 client")

    s3 = boto3.resource('s3',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )
    print("Created s3 client")

    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name='us-west-2'
                      )
    print("Created iam client")
    
    redshift = boto3.client('redshift',
                           region_name="us-west-2",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    print("Created redshift client")
    print("Done - all clients created")
    return(ec2, s3, iam, redshift)
    

    
def create_IAM_role(DWH_IAM_ROLE_NAME, iam):
    '''
    Creates IAM Role and attaches policy, returns the Role ARN
    '''
    try:
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        print("IAM Role created")

    except Exception as e:
        print(e)
   
    print("Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']
    print("Attached Policy")
    
    print("Getting the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print("IAM role ARN: " + roleArn)
    print("Done creating IAM role and attaching policy, Cluster can now be created")
    return(roleArn)
    
def create_redshift_cluster(roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, redshift):
    '''
    Creates the redshift cluster with the specified parameters
    '''
    print("Trying to create a cluster")
    try:
        response = redshift.create_cluster(        
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
        print("Cluster created")
        
    except Exception as e:
        print(e)
        
def prettyRedshiftProps(props):
    '''
    Returns a dataframe with a summary of the cluster parameters that can be displayed
    '''
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])
        

def show_cluster_proportions(DWH_CLUSTER_IDENTIFIER, redshift):
    '''
    Prints the cluster properties as a df and returns the properties
    '''
    print("Showing cluster proportions:")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    df = prettyRedshiftProps(myClusterProps)
    print(df)
    return(myClusterProps)
    

def waitUntil(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Function checks wether 
    '''
    print("Checks if condition (Cluster is available) is met otherwise wait until it is met")
    while (1):
        print("While loop to wait for the cluster to become available")
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        if list(myClusterProps.items())[2][1]=='available': #checks the condition
            print("Condition is met - cluster is available, next stepp will be executed")
            #output
            break
        print("Not available yet, following 40s of waiting until checking again")
        time.sleep(40) #waits 40s for performance
        
def get_endpoint_and_arn(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    Prints out the entpoint/host and role ARN so it can be noted down in the dwh.cfg file.
    '''
    print("Cluster is now available, here are the parameters:")
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    prettyRedshiftProps(myClusterProps)
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    print("WRITE THE DWH_ENDPOINT (=host) AND DWH_ROLE_ARN (=ARN) in the dwh.cfg file to process further with creating tables and etl.py")
    
def open_incoming_TCP_port(DWH_PORT, redshift, ec2, DWH_CLUSTER_IDENTIFIER):
    '''
    Opens an TCP port- to access the cluster endpoint
    '''
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print("Opening and incoming TCP port to access the cluster endpoint")
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        print(defaultSg.group_name)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
        print("TCP Point successfully opened")
    except Exception as e:
        print(e)
        
        
def main():
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME = read_redshift_config()
    
    ec2, s3, iam, redshift = create_clients(KEY, SECRET)
    
    roleArn = create_IAM_role(DWH_IAM_ROLE_NAME, iam)
    
    create_redshift_cluster(roleArn,DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, redshift)
    
    myClusterProps = show_cluster_proportions(DWH_CLUSTER_IDENTIFIER, redshift)
    
    #waiting with the next step until cluster becomes available bevore checking the endpoint and arn
    waitUntil(redshift, DWH_CLUSTER_IDENTIFIER)
    
    get_endpoint_and_arn(redshift, DWH_CLUSTER_IDENTIFIER)
    
    open_incoming_TCP_port(DWH_PORT, redshift, ec2, DWH_CLUSTER_IDENTIFIER)
    
    print("Cluster successfully created in region us-west-2. You can continue with creating the tables and running the etl pipeline. Make sure to write the ARN Role and host in the dwh.cfg file")
    

if __name__ == "__main__":
    main()