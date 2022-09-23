import pandas as pd
import boto3
import json
import configparser
import time

#pd und configparser notwendig?

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
    Creates clients for iam and redshift and returns them
    '''    

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
    return(iam, redshift)


def delete_cluster_and_detach_role(DWH_CLUSTER_IDENTIFIER, redshift, iam, DWH_IAM_ROLE_NAME):
    '''
    Deletes the cluster and detaches the role
    '''
    print("Deleting cluster")
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print("Status of cluster - deletion in process:")
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    print(prettyRedshiftProps(myClusterProps))
    
    print("Detach role")
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print("Role detached")
    
    

def waitUntil(redshift, DWH_CLUSTER_IDENTIFIER):
    '''
    This function waits until the cluster is deleted in order to inform the user that it has been deleted successfully
    '''
    print("Checks if cluster is deleted otherwise waits until its deleted")
    while (1):
        print("While Loop to wait for 'deleted' status")
        try:
              myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        except:
              print("Cluster has been deleted")
              break
        time.sleep(40) 
    
        
def prettyRedshiftProps(props):
    '''
    Returns a dataframe with a summary of the cluster parameters that can be displayed
    '''
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])
        
        
        
def main():
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME = read_redshift_config()
    
    iam, redshift = create_clients(KEY, SECRET)
    
    delete_cluster_and_detach_role(DWH_CLUSTER_IDENTIFIER, redshift, iam, DWH_IAM_ROLE_NAME)
    
    waitUntil(redshift, DWH_CLUSTER_IDENTIFIER)
    print("Process of Deletion and Detaching done")

if __name__ == "__main__":
    main()