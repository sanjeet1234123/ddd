import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
import threading
import webbrowser
from boto3.session import Session
from time import sleep
import logging
from botocore.exceptions import ClientError, TokenRetrievalError, UnauthorizedSSOTokenError
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import queue
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aws_service_probe3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
ACTIVE_REGIONS = ["us-west-2", "us-east-2", "us-east-1", "us-west-1"]
INPUT_FILE = "services.xlsx"
OUTPUT_FILE = "aws-services-audit5.xlsx"
MAX_ACCOUNT_THREADS = 10  # Limit concurrent account threads
MAX_SERVICE_THREADS = 20  # Limit concurrent service threads per account
FLUSH_INTERVAL = 60  # Flush to Excel every 60 seconds

# Lock for synchronizing access to file I/O
file_lock = threading.Lock()

# Queue for collecting results
result_queue = queue.Queue()

# --- SSO Authentication ---
def sso_session():
    """Authenticate using AWS SSO and return an access token and session."""
    logger.info("Starting SSO authentication")
    session = Session()
    start_url = 'https://d-92670ca28f.awsapps.com/start/#/'
    try:
        sso_oidc = session.client('sso-oidc', region_name="us-west-2")
        client_creds = sso_oidc.register_client(clientName='myapp', clientType='public')
        device_authorization = sso_oidc.start_device_authorization(
            clientId=client_creds['clientId'],
            clientSecret=client_creds['clientSecret'],
            startUrl=start_url,
        )
        url = device_authorization['verificationUriComplete']
        device_code = device_authorization['deviceCode']
        expires_in = device_authorization['expiresIn']
        interval = device_authorization['interval']
        webbrowser.open(url, autoraise=True)
        for _ in range(1, expires_in // interval + 1):
            sleep(interval)
            try:
                token = sso_oidc.create_token(
                    grantType='urn:ietf:params:oauth:grant-type:device_code',
                    deviceCode=device_code,
                    clientId=client_creds['clientId'],
                    clientSecret=client_creds['clientSecret'],
                )
                logger.info("SSO authentication successful")
                break
            except sso_oidc.exceptions.AuthorizationPendingException:
                pass
        access_token = token['accessToken']
        return access_token, session
    except Exception as e:
        logger.error(f"Failed to authenticate SSO: {e}", exc_info=True)
        raise

# Get all sessions for accessible accounts
try:
    access_token, base_session = sso_session()
    sso = base_session.client('sso', region_name="us-west-2")
except Exception as e:
    logger.error(f"Exiting due to SSO session failure: {e}")
    sys.exit(1)

def get_account_sessions():
    """Retrieve sessions for all accessible AWS accounts."""
    account_sessions = []
    logger.info("Retrieving account sessions")
    try:
        paginator = sso.get_paginator('list_accounts')
        for page in paginator.paginate(accessToken=access_token):
            for acct in page['accountList']:
                account_id = acct['accountId']
                try:
                    roles = sso.list_account_roles(accessToken=access_token, accountId=account_id)['roleList']
                    if not roles:
                        logger.warning(f"No roles found for account {account_id}")
                        continue
                    role_name = next((r['roleName'] for r in roles if "admin" in r['roleName'].lower()), roles[0]['roleName'])
                    creds = sso.get_role_credentials(
                        accessToken=access_token,
                        accountId=account_id,
                        roleName=role_name
                    )['roleCredentials']
                    session = boto3.Session(
                        aws_access_key_id=creds['accessKeyId'],
                        aws_secret_access_key=creds['secretAccessKey'],
                        aws_session_token=creds['sessionToken']
                    )
                    account_sessions.append((account_id, session))
                    logger.info(f"Added account {account_id} for processing.")
                except Exception as e:
                    logger.error(f"Error retrieving credentials for account {account_id}: {e}", exc_info=True)
                    continue
        logger.info(f"Retrieved sessions for {len(account_sessions)} accounts")
    except Exception as e:
        logger.error(f"Error retrieving account sessions: {e}", exc_info=True)
    return account_sessions

def get_processed_accounts():
    """
    Get a set of account IDs that have already been processed from the Excel file.
    """
    processed_accounts = set()
    if os.path.exists(OUTPUT_FILE):
        try:
            existing_df = pd.read_excel(OUTPUT_FILE)
            if 'AccountId' in existing_df.columns:
                # Convert AccountId to string and get unique values
                processed_accounts = set(existing_df['AccountId'].astype(str).unique())
                logger.info(f"Found {len(processed_accounts)} previously processed accounts")
        except Exception as e:
            logger.error(f"Error reading processed accounts from {OUTPUT_FILE}: {e}", exc_info=True)
    return processed_accounts

# Move the account processing logic to after reading the input file
try:
    logger.info(f"Reading services from {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    services = df.iloc[:, 1].dropna().tolist()
    logger.info(f"Loaded {len(services)} services")
except Exception as e:
    logger.error(f"Error reading Excel file {INPUT_FILE}: {e}", exc_info=True)
    sys.exit(1)

# Get all account sessions
account_sessions = get_account_sessions()

# Get previously processed accounts
processed_accounts = get_processed_accounts()

# Filter out already processed accounts and limit to next 30
remaining_account_sessions = [(account_id, session) 
                            for account_id, session in account_sessions 
                            if account_id not in processed_accounts]
logger.info(f"Found {len(remaining_account_sessions)} unprocessed accounts")

# Take only the next 30 unprocessed accounts
remaining_account_sessions = remaining_account_sessions[:30]
logger.info(f"Processing next {len(remaining_account_sessions)} accounts")

# Service checker function
def check_service(client, service_name):
    """Check if a specific AWS service is in use."""
    try:
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
        elif service_name == 'ecs':
            response = client.list_clusters()
            return len(response.get('clusterArns', [])) > 0
        elif service_name == 'eks':
            response = client.list_clusters()
            return len(response.get('clusters', [])) > 0
        elif service_name == 'ecr':
            response = client.describe_repositories()
            return len(response.get('repositories', [])) > 0
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
        elif service_name == 'dms':
            response = client.describe_replication_instances()
            return len(response.get('ReplicationInstances', [])) > 0
        elif service_name == 'datasync':
            response = client.list_tasks()
            return len(response.get('Tasks', [])) > 0
        elif service_name == 'mgn':
            response = client.describe_source_servers()
            return len(response.get('items', [])) > 0
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
            response = client.get_trace_summaries(
                StartTime=datetime.now() - timedelta(days=1),
                EndTime=datetime.now()
            )
            return len(response.get('TraceSummaries', [])) > 0
        elif service_name == 'fis':
            response = client.list_experiments()
            return len(response.get('experiments', [])) > 0
        elif service_name == 'codeartifact':
            response = client.list_domains()
            return len(response.get('domains', [])) > 0
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
            response = client.list_classification_jobs()
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
    except ClientError as e:
        logger.warning(f"ClientError checking {service_name}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking {service_name}: {e}", exc_info=True)
        return False

# Service mapping
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

# Process a single service for an account
def process_service(service, account_id, session):
    """Process a single service for an account and return the result."""
    try:
        service_id = next((v for k, v in service_map.items() if k.lower() in service.lower()), None)
        if not service_id:
            return {
                'Service': service,
                'Status': 'Check Not Implemented',
                'Region': 'N/A',
                'AccountId': account_id,
                'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

        for region in ACTIVE_REGIONS:
            try:
                logger.debug(f"Checking {service} in {region} for account {account_id}")
                client = session.client(service_id, region_name=region)
                if check_service(client, service_id):
                    return {
                        'Service': service,
                        'Status': 'In Use',
                        'Region': region,
                        'AccountId': account_id,
                        'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
            except (TokenRetrievalError, UnauthorizedSSOTokenError) as e:
                logger.error(f"Token error while checking {service} in {region} for account {account_id}: {e}")
                continue
            except ClientError as e:
                logger.warning(f"Service {service} check failed in {region} for account {account_id}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error checking {service} in {region} for account {account_id}: {e}", exc_info=True)
                continue

        return {
            'Service': service,
            'Status': 'Not In Use',
            'Region': 'Not Found',
            'AccountId': account_id,
            'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        logger.error(f"Error in process_service for {service} in account {account_id}: {e}", exc_info=True)
        return None

# Process all services for a single account
def process_account(account_id, session):
    """Process all services for a single account using multiple threads."""
    logger.info(f"Processing account {account_id}")
    with ThreadPoolExecutor(max_workers=MAX_SERVICE_THREADS) as executor:
        futures = [executor.submit(process_service, service, account_id, session) for service in services]
        for future in as_completed(futures):
            result = future.result()
            if result:
                result_queue.put(result)
    logger.info(f"Finished processing account {account_id}")

def append_to_excel(results_to_append):
    """Append results to the output Excel file."""
    try:
        # Create DataFrame with the correct columns
        columns = ['AccountId', 'Service', 'Status', 'Region', 'Last Checked']
        df_to_append = pd.DataFrame(results_to_append, columns=columns)
        
        with file_lock:
            if os.path.exists(OUTPUT_FILE):
                # Create a backup of the existing file before modifying
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_file = f"{OUTPUT_FILE}.{timestamp}.backup"
                shutil.copy2(OUTPUT_FILE, backup_file)
                logger.info(f"Created backup: {backup_file}")
                
                # Read existing file
                existing_df = pd.read_excel(OUTPUT_FILE)
                
                # Check for duplicates before appending
                # Create a unique key for each row (AccountId + Service)
                existing_df['unique_key'] = existing_df['AccountId'].astype(str) + '_' + existing_df['Service']
                df_to_append['unique_key'] = df_to_append['AccountId'].astype(str) + '_' + df_to_append['Service']
                
                # Filter out rows that already exist
                new_rows = df_to_append[~df_to_append['unique_key'].isin(existing_df['unique_key'])]
                
                if not new_rows.empty:
                    # Remove the unique_key column before saving
                    new_rows = new_rows.drop('unique_key', axis=1)
                    existing_df = existing_df.drop('unique_key', axis=1)
                    
                    # Append new rows to the end of the existing DataFrame
                    combined_df = pd.concat([existing_df, new_rows], ignore_index=True)
                    
                    # Save with xlsxwriter engine for better handling of large files
                    with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
                        combined_df.to_excel(writer, index=False, sheet_name='Sheet1')
                        
                        # Auto-adjust column widths
                        worksheet = writer.sheets['Sheet1']
                        for i, col in enumerate(combined_df.columns):
                            max_length = max(
                                combined_df[col].astype(str).apply(len).max(),
                                len(col)
                            ) + 2
                            worksheet.set_column(i, i, max_length)
                    
                    logger.info(f"Appended {len(new_rows)} new records to {OUTPUT_FILE}")
                else:
                    logger.info("No new records to append - all services for these accounts already processed")
            else:
                # If file doesn't exist, create it with the new data
                with pd.ExcelWriter(OUTPUT_FILE, engine='xlsxwriter') as writer:
                    df_to_append.to_excel(writer, index=False, sheet_name='Sheet1')
                    
                    # Auto-adjust column widths
                    worksheet = writer.sheets['Sheet1']
                    for i, col in enumerate(df_to_append.columns):
                        max_length = max(
                            df_to_append[col].astype(str).apply(len).max(),
                            len(col)
                        ) + 2
                        worksheet.set_column(i, i, max_length)
                
                logger.info(f"Created new file {OUTPUT_FILE} with {len(df_to_append)} records")
    except Exception as e:
        logger.error(f"Error appending to Excel: {e}", exc_info=True)
        # If there was an error, try to restore from backup
        if 'backup_file' in locals() and os.path.exists(backup_file):
            try:
                shutil.copy2(backup_file, OUTPUT_FILE)
                logger.info(f"Restored from backup after error: {backup_file}")
            except Exception as restore_error:
                logger.error(f"Failed to restore from backup: {restore_error}", exc_info=True)

# Add a function to check file size and warn if it's getting too large
def check_file_size():
    """Check the size of the output file and log a warning if it's getting large."""
    if os.path.exists(OUTPUT_FILE):
        size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
        if size_mb > 100:  # Warning at 100MB
            logger.warning(f"Output file is getting large: {size_mb:.2f} MB")
        if size_mb > 500:  # Critical warning at 500MB
            logger.error(f"Output file is very large: {size_mb:.2f} MB. Consider splitting into multiple files.")

# Add this to the flush_to_excel function
def flush_to_excel():
    """Periodically flush results from the queue to the Excel file."""
    while True:
        sleep(FLUSH_INTERVAL)
        results_to_append = []
        while not result_queue.empty():
            results_to_append.append(result_queue.get())
        if results_to_append:
            append_to_excel(results_to_append)
            check_file_size()  # Check file size after each flush

# Start the flush thread
flush_thread = threading.Thread(target=flush_to_excel, daemon=True)
flush_thread.start()

# Main execution
logger.info("Starting AWS Service Probe for remaining accounts")
try:
    with ThreadPoolExecutor(max_workers=MAX_ACCOUNT_THREADS) as executor:
        futures = [executor.submit(process_account, account_id, session) for account_id, session in remaining_account_sessions]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Account thread failed: {e}", exc_info=True)
except KeyboardInterrupt:
    logger.info("Script interrupted by user, flushing remaining data")
except Exception as e:
    logger.error(f"Error processing accounts: {e}", exc_info=True)
finally:
    # Flush any remaining results
    results_to_append = []
    while not result_queue.empty():
        results_to_append.append(result_queue.get())
    if results_to_append:
        append_to_excel(results_to_append)

logger.info("Finished AWS Service Probe")