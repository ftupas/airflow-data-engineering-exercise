import configparser

config = configparser.ConfigParser()
with open("dwh.cfg", "r") as f:
    config.read_file(f)

# AWS Keys and Access
KEY                    = config.get("AWS","KEY")
SECRET                 = config.get("AWS","SECRET")
REGION                 = config.get("AWS","REGION")
IP                     = config.get("AWS","IP")
# DWH
DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
# IAM
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
# ARN
DWH_IAM_ROLE_ARN       = config.get("IAM_ROLE", "ARN")