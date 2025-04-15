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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aws_service_probe1.log'),
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


already_parsed_accounts = ["837945147885","854019576022","909289520024","570962425487","842900648552","604843763707","366605375305","131671678827",
                           "652040280832","148761680968","962376601332","055287386426","028267308726","864688658997","171869335159","120665889631",
                           "980433822493","789703464934","886533543096","811232356401","323246377841","997313743223","847792898868","289553340788",
                           "957067006551","734722513437","851725579488","645220297994","976610513875","625390698835","161529602574","023560456450",
                           "225222043844","226328187653","337909780605","661086191417","093081592932","711387123744","976193263898","638757893227",
                           "510566412683","737087929834","445567072790","391999014620","172673714884","356930707951","231024885929","639573698979",
                           "138422245912","824472476337","147997127915","706038663236","439625909071","481701531908","169584356766","064047601590",
                           "574285669006","423106644568","359667331224","971616958241","975049976911","761425694830","084828569409","337194748992",
                           "780338713958","381491911612","355057502311","901728494433","561669183380","339713064492","311455578062","546201757535",
                           "606045120751","617783654043","021080333687","084828595759","221732351960","865472369996","028760423011","992382726641",
                           "973778929335","937317148736","016079180112","253975681984","343218227375","730335576882","240675401163","347185189228",
                           "186017781120","545038419239","877935079347","374302864308","573906375782","587310260078","842297555285","976675453628",
                           "063638840355","605134434406","407871975061","360370967982","260669291446","383014482315","673536393255","270455637087",
                           "076558560147","056129493299","555443969640","261346494092","011466593612","524312018460","310003914545","784815051481",
                           "217133441568","494330288326","269509328700","891377146660","761018862704","468031837822","587293164303","834228615812",
                           "368968807082","643326331716","244439599368","319302878991","974768957884","105681621771","805395394012","333605782880",
                           "737698818541","676784083184","640168415516","354700212814","018300759195","533267453609","026566289584","890645733144",
                           "940644137888","272427783356","975049890868","782912486201","006763131081","381492258592","470350595694","308393186904",
                           "714729966770","741448961611","346687249423","441168902029","606119815302","198162511755","760474233170","945258302719",
                           "549029141069","778560443489","440554640470","055335016319","992240864529","764353303539","918828843132","753835716815",
                           "482904423492","968504157731","314212634609","928329344310","905418082877","715227889535","578024509648","086726787015",
                           "672820770839","103225329856","508723305511","106980222897","275458229217","864981740412","654654276907","767397975664",
                           "940761539651","026208095312","914197359165","664167166272","211125436877","590183690759","438203369576","193670463418",
                           "893885831021","007389446183","769405062996","338366611847","779846779286","238542225925","709901853272","539247486545",
                           "023876977804","148133402725","560868799257","037710130312","524058650528","094494761430","349944207502","493990407021",
                           "976072353578","002511367482","071798199430","675136609689","679229634720","591545309622","183538417891","212605354415",
                           "888907375722","010466690535","311843163627","667963645006","241533158585","054152494147","058264221266","478588816304",
                           "175782699450","450104753689","537715765911","350335073051","408090186308","551794543194","871413794317","716897854558"]

# Lock for synchronizing access to file I/O
file_lock = threading.Lock()

# Remove existing output file to start fresh each run
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)
    logger.info(f"Removed existing {OUTPUT_FILE}")

# --- SSO Authentication ---
def sso_session():
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
    account_sessions = []
    logger.info("Retrieving account sessions")
    try:
        paginator = sso.get_paginator('list_accounts')
        for page in paginator.paginate(accessToken=access_token):
            for acct in page['accountList']:
                account_id = acct['accountId']
                if account_id in already_parsed_accounts:
                    continue
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

# Read input services list
try:
    logger.info(f"Reading services from {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    services = df.iloc[:, 1].dropna().tolist()
    logger.info(f"Loaded {len(services)} services")
except Exception as e:
    logger.error(f"Error reading Excel file {INPUT_FILE}: {e}", exc_info=True)
    sys.exit(1)

# Retrieve sessions for all accounts
account_sessions = get_account_sessions()

# Service checker function
def check_service(client, service_name):
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
def process_service(service, account_id, session, result_list):
    try:
        service_id = next((v for k, v in service_map.items() if k.lower() in service.lower()), None)
        if not service_id:
            entry = {
                'Service': service,
                'Status': 'Check Not Implemented',
                'Region': 'N/A',
                'AccountId': account_id,
                'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            result_list.append(entry)
            logger.warning(f"Check not implemented for service: {service} in account {account_id}")
            return

        for region in ACTIVE_REGIONS:
            try:
                logger.debug(f"Checking {service} in {region} for account {account_id}")
                client = session.client(service_id, region_name=region)
                if check_service(client, service_id):
                    entry = {
                        'Service': service,
                        'Status': 'In Use',
                        'Region': region,
                        'AccountId': account_id,
                        'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    result_list.append(entry)
                    logger.info(f"Service {service} is in use in {region} for account {account_id}")
                    return
            except (TokenRetrievalError, UnauthorizedSSOTokenError) as e:
                logger.error(f"Token error while checking {service} in {region} for account {account_id}: {e}")
                continue
            except ClientError as e:
                logger.warning(f"Service {service} check failed in {region} for account {account_id}: {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error checking {service} in {region} for account {account_id}: {e}", exc_info=True)
                continue

        entry = {
            'Service': service,
            'Status': 'Not In Use',
            'Region': 'Not Found',
            'AccountId': account_id,
            'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        result_list.append(entry)
        logger.info(f"Service {service} is not in use for account {account_id}")
    except Exception as e:
        logger.error(f"Error in process_service for {service} in account {account_id}: {e}", exc_info=True)

# Append results to Excel file
def append_to_excel(results_to_append):
    try:
        if not results_to_append:
            return
        columns = ['AccountId', 'Service', 'Status', 'Region', 'Last Checked']
        df_to_append = pd.DataFrame(results_to_append, columns=columns)
        if os.path.exists(OUTPUT_FILE):
            existing_df = pd.read_excel(OUTPUT_FILE)
            combined_df = pd.concat([existing_df, df_to_append], ignore_index=True)
            combined_df.to_excel(OUTPUT_FILE, index=False)
        else:
            df_to_append.to_excel(OUTPUT_FILE, index=False)
        logger.info(f"Appended {len(df_to_append)} records for account {results_to_append[0]['AccountId']} to {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Error appending to Excel: {e}", exc_info=True)

# Process all services for a single account
def process_account(account_id, session):
    try:
        logger.info(f"Processing account {account_id}")
        account_results = []
        with ThreadPoolExecutor(max_workers=MAX_SERVICE_THREADS) as executor:
            futures = [executor.submit(process_service, service, account_id, session, account_results) for service in services]
            for future in as_completed(futures):
                try:
                    future.result()  # Propagate exceptions
                except Exception as e:
                    logger.error(f"Service thread failed for account {account_id}: {e}", exc_info=True)
        # Sort results by service name for consistency
        account_results.sort(key=lambda x: x['Service'])
        # Write all results for this account to Excel
        with file_lock:
            append_to_excel(account_results)
        logger.info(f"Finished processing account {account_id}")
    except Exception as e:
        logger.error(f"Error in process_account for {account_id}: {e}", exc_info=True)

# Main execution
logger.info("Starting AWS Service Probe for all accounts")
try:
    with ThreadPoolExecutor(max_workers=MAX_ACCOUNT_THREADS) as executor:
        futures = [executor.submit(process_account, account_id, session) for account_id, session in account_sessions]
        for future in as_completed(futures):
            try:
                future.result()  # Raise exceptions if any
            except Exception as e:
                logger.error(f"Account thread failed: {e}", exc_info=True)
except Exception as e:
    logger.error(f"Error processing accounts: {e}", exc_info=True)

logger.info("Finished AWS Service Probe")