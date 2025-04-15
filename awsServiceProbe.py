import boto3
import pandas as pd
from datetime import datetime
import os

# List of active regions to check. As soon as we find the service in one region,
# we will not check for the other regions.
ACTIVE_REGIONS = ["us-west-2", "us-east-2", "us-east-1", "us-west-1"]

# Path to the input Excel file
INPUT_FILE = "services.xlsx"
# Path for the output Excel file
OUTPUT_FILE = "aws_5g_matrixx_ndc_prod_results.xlsx"

# Check for AWS credentials
required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
    print("Please set the following environment variables:")
    print("export AWS_ACCESS_KEY_ID='your_access_key'")
    print("export AWS_SECRET_ACCESS_KEY='your_secret_key'")
    print("export AWS_SESSION_TOKEN='your_session_token'")
    exit(1)

# Read the Excel file.
# IMPORTANT: This code assumes that the Excel file has a column named "Services".
try:
    df = pd.read_excel(INPUT_FILE)
    # Use the second 'Services' column which contains the actual service names
    services = df.iloc[:, 1].dropna().tolist()  # Get values from second column
except Exception as e:
    print(f"Error reading Excel file: {e}")
    exit(1)

#############################################
# Define service-specific check functions.  #
#############################################

def check_service(client, service_name):
    """
    Generic function to check if a service is being used
    Returns True if the service is in use
    """
    try:
        # Compute Services
        if service_name == 'ec2':
            response = client.describe_instances()
            return len(response.get('Reservations', [])) > 0
        elif service_name == 'lambda':
            response = client.list_functions()
            return len(response.get('Functions', [])) > 0
        elif service_name == 'batch':
            response = client.describe_job_queues()
            return len(response.get('jobQueues', [])) > 0
        elif service_name == 'lightsail':
            response = client.get_instances()
            return len(response.get('instances', [])) > 0
        elif service_name == 'elasticbeanstalk':
            response = client.describe_environments()
            return len(response.get('Environments', [])) > 0
        elif service_name == 'serverlessrepo':
            response = client.list_applications()
            return len(response.get('Applications', [])) > 0
        elif service_name == 'outposts':
            response = client.list_outposts()
            return len(response.get('Outposts', [])) > 0
        elif service_name == 'imagebuilder':
            response = client.list_image_pipelines()
            return len(response.get('imagePipelineList', [])) > 0
        elif service_name == 'apprunner':
            response = client.list_services()
            return len(response.get('ServiceSummaryList', [])) > 0
        elif service_name == 'simspaceweaver':
            response = client.list_simulations()
            return len(response.get('simulations', [])) > 0
        
        # Container Services
        elif service_name == 'ecs':
            response = client.list_clusters()
            return len(response.get('clusterArns', [])) > 0
        elif service_name == 'eks':
            response = client.list_clusters()
            return len(response.get('clusters', [])) > 0
        elif service_name == 'ecr':
            response = client.describe_repositories()
            return len(response.get('repositories', [])) > 0
        
        # Storage Services
        elif service_name == 's3':
            response = client.list_buckets()
            return len(response.get('Buckets', [])) > 0
        elif service_name == 'efs':
            response = client.describe_file_systems()
            return len(response.get('FileSystems', [])) > 0
        elif service_name == 'fsx':
            response = client.describe_file_systems()
            return len(response.get('FileSystems', [])) > 0
        elif service_name == 'glacier':
            response = client.list_vaults()
            return len(response.get('VaultList', [])) > 0
        elif service_name == 'storagegateway':
            response = client.list_gateways()
            return len(response.get('Gateways', [])) > 0
        elif service_name == 'backup':
            response = client.list_backup_vaults()
            return len(response.get('BackupVaultList', [])) > 0
        elif service_name == 'drs':
            response = client.describe_source_servers()
            return len(response.get('items', [])) > 0
        
        # Database Services
        elif service_name == 'rds':
            response = client.describe_db_instances()
            return len(response.get('DBInstances', [])) > 0
        elif service_name == 'dynamodb':
            response = client.list_tables()
            return len(response.get('TableNames', [])) > 0
        elif service_name == 'elasticache':
            response = client.describe_cache_clusters()
            return len(response.get('CacheClusters', [])) > 0
        elif service_name == 'neptune':
            response = client.describe_db_instances()
            return any(instance['Engine'] == 'neptune' for instance in response.get('DBInstances', []))
        elif service_name == 'docdb':
            response = client.describe_db_instances()
            return any(instance['Engine'] == 'docdb' for instance in response.get('DBInstances', []))
        elif service_name == 'qldb':
            response = client.list_ledgers()
            return len(response.get('Ledgers', [])) > 0
        elif service_name == 'keyspaces':
            response = client.list_keyspaces()
            return len(response.get('keyspaces', [])) > 0
        elif service_name == 'timestream-write':
            response = client.list_databases()
            return len(response.get('Databases', [])) > 0
        elif service_name == 'memorydb':
            response = client.list_clusters()
            return len(response.get('Clusters', [])) > 0
        
        # Migration Services
        elif service_name == 'dms':
            response = client.describe_replication_instances()
            return len(response.get('ReplicationInstances', [])) > 0
        elif service_name == 'datasync':
            response = client.list_tasks()
            return len(response.get('Tasks', [])) > 0
        elif service_name == 'mgn':
            response = client.describe_source_servers()
            return len(response.get('items', [])) > 0
        
        # Networking Services
        elif service_name == 'vpc':
            response = client.describe_vpcs()
            return len(response.get('Vpcs', [])) > 0
        elif service_name == 'apigateway':
            response = client.get_rest_apis()
            return len(response.get('items', [])) > 0
        elif service_name == 'route53':
            response = client.list_hosted_zones()
            return len(response.get('HostedZones', [])) > 0
        elif service_name == 'cloudfront':
            response = client.list_distributions()
            return len(response.get('DistributionList', {}).get('Items', [])) > 0
        elif service_name == 'directconnect':
            response = client.describe_connections()
            return len(response.get('connections', [])) > 0
        elif service_name == 'globalaccelerator':
            response = client.list_accelerators()
            return len(response.get('Accelerators', [])) > 0
        
        # Developer Tools
        elif service_name == 'codecommit':
            response = client.list_repositories()
            return len(response.get('repositories', [])) > 0
        elif service_name == 'codebuild':
            response = client.list_projects()
            return len(response.get('projects', [])) > 0
        elif service_name == 'codepipeline':
            response = client.list_pipelines()
            return len(response.get('pipelines', [])) > 0
        elif service_name == 'cloud9':
            response = client.list_environments()
            return len(response.get('environmentIds', [])) > 0
        elif service_name == 'xray':
            response = client.get_trace_summaries(StartTime=datetime.now().replace(days=-1), EndTime=datetime.now())
            return len(response.get('TraceSummaries', [])) > 0
        elif service_name == 'fis':
            response = client.list_experiments()
            return len(response.get('experiments', [])) > 0
        elif service_name == 'codeartifact':
            response = client.list_domains()
            return len(response.get('domains', [])) > 0
        
        # Management Services
        elif service_name == 'cloudwatch':
            response = client.list_metrics()
            return len(response.get('Metrics', [])) > 0
        elif service_name == 'cloudformation':
            response = client.list_stacks()
            return len(response.get('StackSummaries', [])) > 0
        elif service_name == 'cloudtrail':
            response = client.list_trails()
            return len(response.get('Trails', [])) > 0
        elif service_name == 'config':
            response = client.describe_config_rules()
            return len(response.get('ConfigRules', [])) > 0
        elif service_name == 'opsworks':
            response = client.describe_stacks()
            return len(response.get('Stacks', [])) > 0
        elif service_name == 'servicecatalog':
            response = client.list_portfolios()
            return len(response.get('PortfolioDetails', [])) > 0
        elif service_name == 'ssm':
            response = client.list_documents()
            return len(response.get('DocumentIdentifiers', [])) > 0
        elif service_name == 'organizations':
            response = client.list_accounts()
            return len(response.get('Accounts', [])) > 0
        
        # Security Services
        elif service_name == 'iam':
            response = client.list_users()
            return len(response.get('Users', [])) > 0
        elif service_name == 'kms':
            response = client.list_keys()
            return len(response.get('Keys', [])) > 0
        elif service_name == 'secretsmanager':
            response = client.list_secrets()
            return len(response.get('SecretList', [])) > 0
        elif service_name == 'cognito-idp':
            response = client.list_user_pools()
            return len(response.get('UserPools', [])) > 0
        elif service_name == 'guardduty':
            response = client.list_detectors()
            return len(response.get('DetectorIds', [])) > 0
        elif service_name == 'inspector2':
            response = client.list_findings()
            return len(response.get('findings', [])) > 0
        elif service_name == 'macie2':
            response = client.list_classifications_jobs()
            return len(response.get('items', [])) > 0
        elif service_name == 'sso':
            response = client.list_instances()
            return len(response.get('Instances', [])) > 0
        elif service_name == 'acm':
            response = client.list_certificates()
            return len(response.get('CertificateSummaryList', [])) > 0
        elif service_name == 'waf':
            response = client.list_web_acls()
            return len(response.get('WebACLs', [])) > 0
        elif service_name == 'shield':
            response = client.list_protections()
            return len(response.get('Protections', [])) > 0
        elif service_name == 'securityhub':
            response = client.list_findings()
            return len(response.get('Findings', [])) > 0
        
        # Application Integration
        elif service_name == 'sns':
            response = client.list_topics()
            return len(response.get('Topics', [])) > 0
        elif service_name == 'sqs':
            response = client.list_queues()
            return len(response.get('QueueUrls', [])) > 0
        elif service_name == 'events':
            response = client.list_rules()
            return len(response.get('Rules', [])) > 0
        elif service_name == 'stepfunctions':
            response = client.list_state_machines()
            return len(response.get('stateMachines', [])) > 0
        elif service_name == 'mq':
            response = client.list_brokers()
            return len(response.get('BrokerSummaries', [])) > 0
        
        # Analytics Services
        elif service_name == 'athena':
            response = client.list_work_groups()
            return len(response.get('WorkGroups', [])) > 0
        elif service_name == 'redshift':
            response = client.describe_clusters()
            return len(response.get('Clusters', [])) > 0
        elif service_name == 'opensearch':
            response = client.list_domain_names()
            return len(response.get('DomainNames', [])) > 0
        elif service_name == 'kinesis':
            response = client.list_streams()
            return len(response.get('StreamNames', [])) > 0
        elif service_name == 'quicksight':
            response = client.list_users()
            return len(response.get('UserList', [])) > 0
        elif service_name == 'glue':
            response = client.get_databases()
            return len(response.get('DatabaseList', [])) > 0
        elif service_name == 'firehose':
            response = client.list_delivery_streams()
            return len(response.get('DeliveryStreamNames', [])) > 0
        
        return False
    except Exception as e:
        print(f"Error checking {service_name}: {e}")
        return False

#############################################
# Main loop: check for each service         #
#############################################

# Dictionary to store results
results = []

# Service name mapping
service_map = {
    'EC2': 'ec2',
    'Lambda': 'lambda',
    'Batch': 'batch',
    'Lightsail': 'lightsail',
    'Elastic Beanstalk': 'elasticbeanstalk',
    'Serverless Application Repository': 'serverlessrepo',
    'AWS Outposts': 'outposts',
    'EC2 Image Builder': 'imagebuilder',
    'AWS App Runner': 'apprunner',
    'AWS SimSpace Weaver': 'simspaceweaver',
    'Elastic Container Service': 'ecs',
    'Elastic Kubernetes Service': 'eks',
    'Elastic Container Registry': 'ecr',
    'S3': 's3',
    'EFS': 'efs',
    'FSx': 'fsx',
    'S3 Glacier': 'glacier',
    'Storage Gateway': 'storagegateway',
    'AWS Backup': 'backup',
    'AWS Elastic Disaster Recovery': 'drs',
    'Aurora and RDS': 'rds',
    'DynamoDB': 'dynamodb',
    'ElastiCache': 'elasticache',
    'Neptune': 'neptune',
    'Amazon QLDB': 'qldb',
    'Amazon DocumentDB': 'docdb',
    'Amazon Keyspaces': 'keyspaces',
    'Amazon Timestream': 'timestream-write',
    'Amazon MemoryDB': 'memorydb',
    'Database Migration Service': 'dms',
    'DataSync': 'datasync',
    'AWS Application Migration Service': 'mgn',
    'VPC': 'vpc',
    'API Gateway': 'apigateway',
    'Route 53': 'route53',
    'CloudFront': 'cloudfront',
    'Direct Connect': 'directconnect',
    'Global Accelerator': 'globalaccelerator',
    'CodeCommit': 'codecommit',
    'CodeBuild': 'codebuild',
    'CodePipeline': 'codepipeline',
    'Cloud9': 'cloud9',
    'X-Ray': 'xray',
    'AWS FIS': 'fis',
    'CodeArtifact': 'codeartifact',
    'CloudWatch': 'cloudwatch',
    'CloudFormation': 'cloudformation',
    'CloudTrail': 'cloudtrail',
    'AWS Config': 'config',
    'OpsWorks': 'opsworks',
    'Service Catalog': 'servicecatalog',
    'Systems Manager': 'ssm',
    'AWS Organizations': 'organizations',
    'IAM': 'iam',
    'Key Management Service': 'kms',
    'Secrets Manager': 'secretsmanager',
    'Cognito': 'cognito-idp',
    'GuardDuty': 'guardduty',
    'Amazon Inspector': 'inspector2',
    'Amazon Macie': 'macie2',
    'IAM Identity Center': 'sso',
    'Certificate Manager': 'acm',
    'WAF & Shield': 'waf',
    'Security Hub': 'securityhub',
    'Simple Notification Service': 'sns',
    'Simple Queue Service': 'sqs',
    'Amazon EventBridge': 'events',
    'Step Functions': 'stepfunctions',
    'Amazon MQ': 'mq',
    'Athena': 'athena',
    'Amazon Redshift': 'redshift',
    'Amazon OpenSearch Service': 'opensearch',
    'Kinesis': 'kinesis',
    'QuickSight': 'quicksight',
    'AWS Glue': 'glue',
    'Amazon Data Firehose': 'firehose'
}

# Check each service
for service in services:
    print(f"\nChecking usage for service: {service}")
    service_in_use = False
    used_region = "Not Found"
    
    # Get the AWS service identifier
    service_id = None
    for key, value in service_map.items():
        if key.lower() in service.lower():
            service_id = value
            break
    
    if not service_id:
        print(f"  [!] No check implemented for service '{service}'. Skipping...")
        results.append({
            'Service': service,
            'Status': 'Check Not Implemented',
            'Region': 'N/A',
            'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        continue

    # Check each region for the service
    for region in ACTIVE_REGIONS:
        print(f"  -> Checking region: {region}")
        try:
            client = boto3.client(service_id, region_name=region)
            if check_service(client, service_id):
                print(f"     [+] Found usage of service '{service}' in region {region}")
                service_in_use = True
                used_region = region
                break
            else:
                print(f"     [-] No usage of service '{service}' in region {region}")
        except Exception as e:
            print(f"     Error checking service '{service}' in region {region}: {e}")
            continue

    # Store results
    results.append({
        'Service': service,
        'Status': 'In Use' if service_in_use else 'Not In Use',
        'Region': used_region,
        'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

# Create DataFrame with results
results_df = pd.DataFrame(results)

# Add results columns to the original DataFrame
df['Status'] = pd.Series(dtype='object')  # Initialize new columns
df['Region'] = pd.Series(dtype='object')
df['Last Checked'] = pd.Series(dtype='object')

# Update the results in the rows where we have data
for idx, row in results_df.iterrows():
    mask = df.iloc[:, 1] == row['Service']  # Match on the second 'Services' column
    if any(mask):
        df.loc[mask, 'Status'] = row['Status']
        df.loc[mask, 'Region'] = row['Region']
        df.loc[mask, 'Last Checked'] = row['Last Checked']

# Save updated results back to Excel
try:
    df.to_excel(OUTPUT_FILE, index=False)
    print(f"\nResults have been saved to: {OUTPUT_FILE}")
except Exception as e:
    print(f"Error saving results to Excel: {e}")
