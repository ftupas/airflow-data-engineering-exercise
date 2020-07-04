from create_cluster import time, create_aws_client, get_cluster_info
from process_config import REGION, KEY, SECRET, DWH_CLUSTER_IDENTIFIER

def delete_redshift_cluster(redshift):
    """
    Deletes the redshift cluster
    
    Params:
        redshift(object): redshift(object): Redshift client that was created with AWS Keys and Secrets
    """
    print(f"DELETING {DWH_CLUSTER_IDENTIFIER}")
    try:
        redshift.delete_cluster(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot = True)
    except Exception as e:
        print(e)

def main():
    """
    Function used to delete a Redshift cluster.
    Usage: python.exe delete_cluster.py Windows
           python     delete_cluster.py Linux/Mac 
    """
    # create redshift client
    redshift = create_aws_client("redshift")

    # delete redshift cluster
    delete_redshift_cluster(redshift)

    while True:
        time.sleep(10)
        try:
            response = get_cluster_info(redshift)
            status = response["ClusterStatus"]

            if status.lower() == "deleting":
                print("DELETING...")
                continue
            else:
                break
        except Exception:
            break

    print("DONE!")

if __name__ == "__main__":
    main()