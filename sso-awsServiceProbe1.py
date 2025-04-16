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
        logging.FileHandler('aws_service_probe3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
ACTIVE_REGIONS = ["us-west-2", "us-east-2", "us-east-1", "us-west-1"]
INPUT_FILE = "services.xlsx"
OUTPUT_FILE = "aws-services-audit2.xlsx"
MAX_ACCOUNT_THREADS = 5  # Limit concurrent account threads
MAX_SERVICE_THREADS = 10  # Limit concurrent service threads per account

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
                # Filter for only the specified account
                if account_id != "721526942678":
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
        elif service_name == 'amplify':
            response = client.list_apps()
            return len(response.get('apps', [])) > 0
        elif service_name == 'appmesh':
            response = client.list_meshes()
            return len(response.get('meshes', [])) > 0
        elif service_name == 'appconfig':
            response = client.list_applications()
            return len(response.get('Items', [])) > 0
        elif service_name == 'appfabric':
            response = client.list_app_bundles()
            return len(response.get('appBundleSummaries', [])) > 0
        elif service_name == 'appsync':
            response = client.list_graphql_apis()
            return len(response.get('graphqlApis', [])) > 0
        elif service_name == 'artifact':
            response = client.list_reports()
            return len(response.get('Reports', [])) > 0
        elif service_name == 'auditmanager':
            response = client.list_assessments()
            return len(response.get('assessmentMetadataList', [])) > 0
        elif service_name == 'autoscaling':
            response = client.describe_auto_scaling_groups()
            return len(response.get('AutoScalingGroups', [])) > 0
        elif service_name == 'b2bi':
            response = client.list_partners()
            return len(response.get('partners', [])) > 0
        elif service_name == 'billingconductor':
            response = client.list_billing_groups()
            return len(response.get('BillingGroups', [])) > 0
        elif service_name == 'cleanrooms':
            response = client.list_collaborations()
            return len(response.get('collaborationList', [])) > 0
        elif service_name == 'servicediscovery':
            response = client.list_services()
            return len(response.get('Services', [])) > 0
        elif service_name == 'compute-optimizer':
            response = client.get_enrollment_status()
            return response.get('status') == 'Active'
        elif service_name == 'dataexchange':
            response = client.list_data_sets()
            return len(response.get('DataSets', [])) > 0
        # Adding checks for the new services
        elif service_name == 'panorama':
            response = client.list_devices()
            return len(response.get('Devices', [])) > 0
        elif service_name == 'payment-cryptography':
            response = client.list_hsm_clients()
            return len(response.get('HsmClients', [])) > 0
        elif service_name == 'private-5g':
            response = client.list_5g_core_networks()
            return len(response.get('CoreNetworks', [])) > 0
        elif service_name == 'private-ca':
            response = client.list_certificate_authorities()
            return len(response.get('CertificateAuthorities', [])) > 0
        elif service_name == 'proton':
            response = client.list_services()
            return len(response.get('services', [])) > 0
        elif service_name == 'resiliencehub':
            response = client.list_applications()
            return len(response.get('applications', [])) > 0
        elif service_name == 'resource-explorer':
            response = client.list_indexes()
            return len(response.get('Indexes', [])) > 0
        elif service_name == 'robomaker':
            response = client.list_robots()
            return len(response.get('robots', [])) > 0
        elif service_name == 'security-incident-response':
            response = client.list_incidents()
            return len(response.get('incidentIds', [])) > 0
        elif service_name == 'signer':
            response = client.list_signing_jobs()
            return len(response.get('jobs', [])) > 0
        elif service_name == 'snow-family':
            response = client.list_jobs()
            return len(response.get('Jobs', [])) > 0
        elif service_name == 'supply-chain':
            response = client.list_supply_chains()
            return len(response.get('SupplyChains', [])) > 0
        elif service_name == 'telco-network-builder':
            response = client.list_networks()
            return len(response.get('Networks', [])) > 0
        elif service_name == 'transfer-family':
            response = client.list_servers()
            return len(response.get('Servers', [])) > 0
        elif service_name == 'user-notifications':
            response = client.list_subscriptions()
            return len(response.get('Subscriptions', [])) > 0
        elif service_name == 'well-architected':
            response = client.list_workloads()
            return len(response.get('WorkloadSummaries', [])) > 0
        elif service_name == 'wickr':
            response = client.list_teams()
            return len(response.get('Teams', [])) > 0
        elif service_name == 'repost-private':
            response = client.list_posts()
            return len(response.get('Posts', [])) > 0
        elif service_name == 'activate-for-startups':
            response = client.list_startups()
            return len(response.get('Startups', [])) > 0
        elif service_name == 'appflow':
            response = client.list_flows()
            return len(response.get('Flows', [])) > 0
        elif service_name == 'augmented-ai':
            response = client.list_human_tasks()
            return len(response.get('HumanTasks', [])) > 0
        elif service_name == 'bedrock':
            response = client.list_foundations()
            return len(response.get('Foundations', [])) > 0
        elif service_name == 'braket':
            response = client.list_quantum_processors()
            return len(response.get('QuantumProcessors', [])) > 0
        elif service_name == 'chime':
            response = client.list_users()
            return len(response.get('Users', [])) > 0
        elif service_name == 'chime-sdk':
            response = client.list_channels()
            return len(response.get('Channels', [])) > 0
        elif service_name == 'codecatalyst':
            response = client.list_projects()
            return len(response.get('projects', [])) > 0
        elif service_name == 'codeguru':
            response = client.list_repositories()
            return len(response.get('repositories', [])) > 0
        elif service_name == 'comprehend':
            response = client.list_entities_detection_jobs()
            return len(response.get('JobList', [])) > 0
        elif service_name == 'comprehend-medical':
            response = client.list_entities_detection_jobs()
            return len(response.get('JobList', [])) > 0
        elif service_name == 'connect':
            response = client.list_instances()
            return len(response.get('InstanceSummaryList', [])) > 0
        elif service_name == 'datazone':
            response = client.list_domains()
            return len(response.get('Domains', [])) > 0
        elif service_name == 'devops-guru':
            response = client.list_anomalies()
            return len(response.get('Anomalies', [])) > 0
        elif service_name == 'elastic-vmware-service':
            response = client.list_virtual_machines()
            return len(response.get('VirtualMachines', [])) > 0
        elif service_name == 'finspace':
            response = client.list_environments()
            return len(response.get('Environments', [])) > 0
        elif service_name == 'forecast':
            response = client.list_forecasts()
            return len(response.get('Forecasts', [])) > 0
        elif service_name == 'fraud-detector':
            response = client.list_detectors()
            return len(response.get('Detectors', [])) > 0
        elif service_name == 'gamelift-servers':
            response = client.list_game_sessions()
            return len(response.get('GameSessions', [])) > 0
        elif service_name == 'gamelift-streams':
            response = client.list_streams()
            return len(response.get('Streams', [])) > 0
        elif service_name == 'grafana':
            response = client.list_workspaces()
            return len(response.get('Workspaces', [])) > 0
        elif service_name == 'interactive-video-service':
            response = client.list_streams()
            return len(response.get('Streams', [])) > 0
        elif service_name == 'kendra':
            response = client.list_indices()
            return len(response.get('Indices', [])) > 0
        elif service_name == 'lex':
            response = client.list_intents()
            return len(response.get('Intents', [])) > 0
        elif service_name == 'location':
            response = client.list_geofence_collections()
            return len(response.get('GeofenceCollections', [])) > 0
        elif service_name == 'lookout-for-equipment':
            response = client.list_alarms()
            return len(response.get('Alarms', [])) > 0
        elif service_name == 'lookout-for-metrics':
            response = client.list_alerts()
            return len(response.get('Alerts', [])) > 0
        elif service_name == 'lookout-for-vision':
            response = client.list_datasets()
            return len(response.get('Datasets', [])) > 0
        elif service_name == 'managed-blockchain':
            response = client.list_networks()
            return len(response.get('Networks', [])) > 0
        elif service_name == 'monitron':
            response = client.list_projects()
            return len(response.get('Projects', [])) > 0
        elif service_name == 'one-enterprise':
            response = client.list_devices()
            return len(response.get('Devices', [])) > 0
        elif service_name == 'personalize':
            response = client.list_datasets()
            return len(response.get('Datasets', [])) > 0
        elif service_name == 'pinpoint':
            response = client.list_campaigns()
            return len(response.get('Campaigns', [])) > 0
        elif service_name == 'polly':
            response = client.describe_voices()
            return len(response.get('Voices', [])) > 0
        elif service_name == 'prometheus':
            response = client.list_workspace_summaries()
            return len(response.get('WorkspaceSummaries', [])) > 0
        elif service_name == 'q':
            response = client.list_sessions()
            return len(response.get('Sessions', [])) > 0
        elif service_name == 'q-business':
            response = client.list_sessions()
            return len(response.get('Sessions', [])) > 0
        elif service_name == 'q-developer':
            response = client.list_sessions()
            return len(response.get('Sessions', [])) > 0
        elif service_name == 'q-developer-chat':
            response = client.list_chat_sessions()
            return len(response.get('ChatSessions', [])) > 0
        elif service_name == 'rekognition':
            response = client.list_collections()
            return len(response.get('CollectionIds', [])) > 0
        elif service_name == 'sagemaker':
            response = client.list_notebook_instances()
            return len(response.get('NotebookInstances', [])) > 0
        elif service_name == 'sagemaker-ai':
            response = client.list_human_tasks()
            return len(response.get('HumanTasks', [])) > 0
        elif service_name == 'simple-email-service':
            response = client.list_smtp_credentials()
            return len(response.get('SmtpCredentials', [])) > 0
        elif service_name == 'textract':
            response = client.list_documents()
            return len(response.get('Documents', [])) > 0
        elif service_name == 'transcribe':
            response = client.list_transcription_jobs()
            return len(response.get('TranscriptionJobSummaries', [])) > 0
        elif service_name == 'translate':
            response = client.list_text_translation_jobs()
            return len(response.get('TextTranslationJobSummaries', [])) > 0
        elif service_name == 'verified-permissions':
            response = client.list_permissions()
            return len(response.get('Permissions', [])) > 0
        elif service_name == 'workdocs':
            response = client.list_users()
            return len(response.get('Users', [])) > 0
        elif service_name == 'workmail':
            response = client.list_organizations()
            return len(response.get('Organizations', [])) > 0
        elif service_name == 'analytics':
            response = client.list_datasets()
            return len(response.get('Datasets', [])) > 0
        elif service_name == 'appstream':
            response = client.list_fleets()
            return len(response.get('Fleets', [])) > 0
        elif service_name == 'application-discovery':
            response = client.list_discovered_resources()
            return len(response.get('Resources', [])) > 0
        elif service_name == 'application-integration':
            response = client.list_integrations()
            return len(response.get('Integrations', [])) > 0
        elif service_name == 'application-recovery-controller':
            response = client.list_recovery_groups()
            return len(response.get('RecoveryGroups', [])) > 0
        elif service_name == 'aurora-dsql':
            response = client.describe_db_clusters()
            return len(response.get('DBClusters', [])) > 0
        elif service_name == 'billing-and-cost-management':
            response = client.describe_cost_and_usage()
            return len(response.get('ResultsByTime', [])) > 0
        elif service_name == 'blockchain':
            response = client.list_networks()
            return len(response.get('Networks', [])) > 0
        elif service_name == 'business-applications':
            response = client.list_applications()
            return len(response.get('Applications', [])) > 0
        elif service_name == 'cloud-financial-management':
            response = client.describe_budgets()
            return len(response.get('Budgets', [])) > 0
        # elif service_name == 'cloudhsm':
        #     response = client.list_hsms()
            return len(response.get('Hsms', [])) > 0
        elif service_name == 'cloudsearch':
            response = client.describe_domains()
            return len(response.get('SearchServiceDomains', [])) > 0
        elif service_name == 'cloudshell':
            response = client.describe_sessions()
            return len(response.get('Sessions', [])) > 0
        elif service_name == 'codedeploy':
            response = client.list_deployments()
            return len(response.get('Deployments', [])) > 0
        elif service_name == 'control-tower':
            response = client.list_enabled_controls()
            return len(response.get('EnabledControls', [])) > 0
        elif service_name == 'customer-enablement':
            response = client.list_enablement_plans()
            return len(response.get('EnablementPlans', [])) > 0
        elif service_name == 'detective':
            response = client.list_graphs()
            return len(response.get('Graphs', [])) > 0
        elif service_name == 'developer-tools':
            response = client.list_code_repositories()
            return len(response.get('Repositories', [])) > 0
        elif service_name == 'device-farm':
            response = client.list_device_pools()
            return len(response.get('DevicePools', [])) > 0
        elif service_name == 'directory-service':
            response = client.describe_directories()
            return len(response.get('DirectoryDescriptions', [])) > 0
        elif service_name == 'emr':
            response = client.list_clusters()
            return len(response.get('Clusters', [])) > 0
        elif service_name == 'elastic-transcoder':
            response = client.list_pipelines()
            return len(response.get('Pipelines', [])) > 0
        elif service_name == 'elemental-appliances-software':
            response = client.list_devices()
            return len(response.get('Devices', [])) > 0
        elif service_name == 'end-user-computing':
            response = client.list_applications()
            return len(response.get('Applications', [])) > 0
        elif service_name == 'front-end-web-mobile':
            response = client.list_web_apps()
            return len(response.get('WebApps', [])) > 0
        elif service_name == 'game-development':
            response = client.list_games()
            return len(response.get('Games', [])) > 0
        elif service_name == 'ground-station':
            response = client.list_dataflow_edges()
            return len(response.get('DataflowEdges', [])) > 0
        elif service_name == 'incident-manager':
            response = client.list_incidents()
            return len(response.get('Incidents', [])) > 0
        elif service_name == 'infrastructure-composer':
            response = client.list_stacks()
            return len(response.get('Stacks', [])) > 0
        elif service_name == 'internet-of-things':
            response = client.list_thing_groups()
            return len(response.get('ThingGroups', [])) > 0
        elif service_name == 'iot-analytics':
            response = client.list_datasets()
            return len(response.get('Datasets', [])) > 0
        elif service_name == 'iot-core':
            response = client.list_things()
            return len(response.get('Things', [])) > 0
        elif service_name == 'iot-device-defender':
            response = client.list_detectors()
            return len(response.get('Detectors', [])) > 0
        elif service_name == 'iot-device-management':
            response = client.list_thing_groups()
            return len(response.get('ThingGroups', [])) > 0
        elif service_name == 'iot-events':
            response = client.list_inputs()
            return len(response.get('Inputs', [])) > 0
        elif service_name == 'iot-greengrass':
            response = client.list_groups()
            return len(response.get('Groups', [])) > 0
        elif service_name == 'iot-sitewise':
            response = client.list_assets()
            return len(response.get('Assets', [])) > 0
        elif service_name == 'iot-twinmaker':
            response = client.list_entities()
            return len(response.get('Entities', [])) > 0
        elif service_name == 'launch-wizard':
            response = client.list_projects()
            return len(response.get('Projects', [])) > 0
        elif service_name == 'msk':
            response = client.list_clusters()
            return len(response.get('Clusters', [])) > 0
        elif service_name == 'machine-learning':
            response = client.list_models()
            return len(response.get('Models', [])) > 0
        elif service_name == 'managed-apache-airflow':
            response = client.list_environments()
            return len(response.get('Environments', [])) > 0
        elif service_name == 'managed-apache-flink':
            response = client.list_environments()
            return len(response.get('Environments', [])) > 0
        elif service_name == 'managed-services':
            response = client.list_services()
            return len(response.get('Services', [])) > 0
        elif service_name == 'management-governance':
            response = client.list_controls()
            return len(response.get('Controls', [])) > 0
        elif service_name == 'media-services':
            response = client.list_assets()
            return len(response.get('Assets', [])) > 0
        elif service_name == 'mediaconnect':
            response = client.list_flows()
            return len(response.get('Flows', [])) > 0
        elif service_name == 'mediaconvert':
            response = client.list_jobs()
            return len(response.get('Jobs', [])) > 0
        elif service_name == 'medialive':
            response = client.list_channels()
            return len(response.get('Channels', [])) > 0
        elif service_name == 'mediapackage':
            response = client.list_channels()
            return len(response.get('Channels', [])) > 0
        elif service_name == 'mediastore':
            response = client.list_containers()
            return len(response.get('Containers', [])) > 0
        elif service_name == 'mediatailor':
            response = client.list_channels()
            return len(response.get('Channels', [])) > 0
        elif service_name == 'migration-transfer':
            response = client.list_transfers()
            return len(response.get('Transfers', [])) > 0
        elif service_name == 'oracle-database-aws':
            response = client.describe_db_instances()
            return len(response.get('DBInstances', [])) > 0
        elif service_name == 'parallel-computing-service':
            response = client.list_computing_clusters()
            return len(response.get('ComputingClusters', [])) > 0
        elif service_name == 'quantum-technologies':
            response = client.list_quantum_tasks()
            return len(response.get('QuantumTasks', [])) > 0
        elif service_name == 'red-hat-openshift-service':
            response = client.list_clusters()
            return len(response.get('Clusters', [])) > 0
        elif service_name == 'resource-access-manager':
            response = client.list_resources()
            return len(response.get('Resources', [])) > 0
        elif service_name == 'resource-groups-tag-editor':
            response = client.list_groups()
            return len(response.get('Groups', [])) > 0
        elif service_name == 'robotics':
            response = client.list_robotics_jobs()
            return len(response.get('RoboticsJobs', [])) > 0
        elif service_name == 'swf':
            response = client.list_workflows()
            return len(response.get('Workflows', [])) > 0
        elif service_name == 'satellite':
            response = client.list_satellites()
            return len(response.get('Satellites', [])) > 0
        elif service_name == 'security-lake':
            response = client.list_data_sources()
            return len(response.get('DataSources', [])) > 0
        elif service_name == 'service-quotas':
            response = client.list_services()
            return len(response.get('Services', [])) > 0
        elif service_name == 'support':
            response = client.describe_cases()
            return len(response.get('Cases', [])) > 0
        elif service_name == 'trusted-advisor':
            response = client.describe_check_results()
            return len(response.get('CheckResults', [])) > 0
        elif service_name == 'workspaces':
            response = client.describe_workspaces()
            return len(response.get('Workspaces', [])) > 0
        elif service_name == 'workspaces-secure-browser':
            response = client.describe_secure_browsers()
            return len(response.get('SecureBrowsers', [])) > 0
        elif service_name == 'workspaces-thin-client':
            response = client.describe_thin_clients()
            return len(response.get('ThinClients', [])) > 0
        elif service_name == 'deadline':
            response = client.list_farms()
            farms = response.get('farms', [])
            return len(farms) > 0
        elif service_name == 'deepcomposer':
            response = client.list_compositions()
            return len(response.get('compositions', [])) > 0
        elif service_name == 'pinpoint-sms-voice-v2':
            response = client.list_configuration_sets()
            return len(response.get('ConfigurationSets', [])) > 0
        elif service_name == 'entityresolution':
            response = client.list_matching_workflows()
            return len(response.get('matchingWorkflows', [])) > 0
        elif service_name == 'fms':
            response = client.list_policies()
            return len(response.get('PolicyList', [])) > 0
        elif service_name == 'health':
            response = client.describe_events()
            return len(response.get('events', [])) > 0
        elif service_name == 'medical-imaging':
            response = client.list_datastores()
            return len(response.get('datastoreSummaries', [])) > 0
        elif service_name == 'healthlake':
            response = client.list_fhir_datastores()
            return len(response.get('DatastorePropertiesList', [])) > 0
        elif service_name == 'omics':
            response = client.list_runs()
            return len(response.get('runs', [])) > 0
        elif service_name == 'iotfleetwise':
            response = client.list_vehicles()
            return len(response.get('vehicles', [])) > 0
        elif service_name == 'lakeformation':
            response = client.list_lf_tags()
            return len(response.get('LFTags', [])) > 0
        elif service_name == 'license-manager':
            response = client.list_licenses()
            return len(response.get('Licenses', [])) > 0
        elif service_name == 'm2':
            response = client.list_applications()
            return len(response.get('applications', [])) > 0
        elif service_name == 'marketplace-catalog':
            response = client.list_entities(EntityType='AmiProduct')
            return len(response.get('EntitySummaryList', [])) > 0
        elif service_name == 'mgh':
            response = client.list_migration_tasks()
            return len(response.get('MigrationTaskSummaryList', [])) > 0
        elif service_name == 'pinpoint-sms-voice-v2':
            response = client.list_configuration_sets()
            return len(response.get('ConfigurationSets', [])) > 0
        elif service_name == 'entityresolution':
            response = client.list_matching_workflows()
            return len(response.get('matchingWorkflows', [])) > 0
  
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
    'Amazon Data Firehose': 'firehose',
    'AWS Amplify': 'amplify',
    'AWS App Mesh': 'appmesh',
    'AWS AppConfig': 'appconfig',
    'AWS AppFabric': 'appfabric',
    'AWS AppSync': 'appsync',
    'AWS Artifact': 'artifact',
    'AWS Audit Manager': 'auditmanager',
    'AWS Auto Scaling': 'autoscaling',
    'AWS B2B Data Interchange': 'b2bi',
    'AWS Billing Conductor': 'billingconductor',
    'AWS Clean Rooms': 'cleanrooms',
    'AWS Cloud Map': 'servicediscovery',
    'AWS Compute Optimizer': 'compute-optimizer',
    'AWS Data Exchange': 'dataexchange',
    'AWS Panorama': 'panorama',
    'AWS Payment Cryptography': 'payment-cryptography',
    'AWS Private 5G': 'private-5g',
    'AWS Private Certificate Authority': 'private-ca',
    'AWS Proton': 'proton',
    'AWS Resilience Hub': 'resiliencehub',
    'AWS Resource Explorer': 'resource-explorer',
    'AWS RoboMaker': 'robomaker',
    'AWS Security Incident Response': 'security-incident-response',
    'AWS Signer': 'signer',
    'AWS Snow Family': 'snow-family',
    'AWS Supply Chain': 'supply-chain',
    'AWS Telco Network Builder': 'telco-network-builder',
    'AWS Transfer Family': 'transfer-family',
    'AWS User Notifications': 'user-notifications',
    'AWS Well-Architected Tool': 'well-architected',
    'AWS Wickr': 'wickr',
    'AWS re:Post Private': 'repost-private',
    'Activate for Startups': 'activate-for-startups',
    'Amazon AppFlow': 'appflow',
    'Amazon Augmented AI': 'augmented-ai',
    'Amazon Bedrock': 'bedrock',
    'Amazon Braket': 'braket',
    'Amazon Chime': 'chime',
    'Amazon Chime SDK': 'chime-sdk',
    'Amazon CodeCatalyst': 'codecatalyst',
    'Amazon CodeGuru': 'codeguru',
    'Amazon Comprehend': 'comprehend',
    'Amazon Comprehend Medical': 'comprehend-medical',
    'Amazon Connect': 'connect',
    'Amazon DataZone': 'datazone',
    'Amazon DevOps Guru': 'devops-guru',
    'Amazon Elastic VMware Service (Preview)': 'elastic-vmware-service',
    'Amazon FinSpace': 'finspace',
    'Amazon Forecast': 'forecast',
    'Amazon Fraud Detector': 'fraud-detector',
    'Amazon GameLift Servers': 'gamelift-servers',
    'Amazon GameLift Streams': 'gamelift-streams',
    'Amazon Grafana': 'grafana',
    'Amazon Interactive Video Service': 'interactive-video-service',
    'Amazon Kendra': 'kendra',
    'Amazon Lex': 'lex',
    'Amazon Location Service': 'location',
    'Amazon Lookout for Equipment': 'lookout-for-equipment',
    'Amazon Lookout for Metrics': 'lookout-for-metrics',
    'Amazon Lookout for Vision': 'lookout-for-vision',
    'Amazon Managed Blockchain': 'managed-blockchain',
    'Amazon Monitron': 'monitron',
    'Amazon One Enterprise': 'one-enterprise',
    'Amazon Personalize': 'personalize',
    'Amazon Pinpoint': 'pinpoint',
    'Amazon Polly': 'polly',
    'Amazon Prometheus': 'prometheus',
    'Amazon Q': 'q',
    'Amazon Q Business': 'q-business',
    'Amazon Q Developer (Including Amazon CodeWhisperer)': 'q-developer',
    'Amazon Q Developer in chat applications (previously AWS Chatbot)': 'q-developer-chat',
    'Amazon Rekognition': 'rekognition',
    'Amazon SageMaker': 'sagemaker',
    'Amazon SageMaker AI': 'sagemaker-ai',
    'Amazon Simple Email Service': 'simple-email-service',
    'Amazon Textract': 'textract',
    'Amazon Transcribe': 'transcribe',
    'Amazon Translate': 'translate',
    'Amazon Verified Permissions': 'verified-permissions',
    'Amazon WorkDocs': 'workdocs',
    'Amazon WorkMail': 'workmail',
    'Analytics': 'analytics',
    'AppStream 2.0': 'appstream',
    'Application Discovery Service': 'application-discovery',
    'Application Integration': 'application-integration',
    'Application Recovery Controller': 'application-recovery-controller',
    'Aurora DSQL': 'aurora-dsql',
    'Billing and Cost Management': 'billing-and-cost-management',
    'Blockchain': 'blockchain',
    'Business Applications': 'business-applications',
    'Cloud Financial Management': 'cloud-financial-management',
    # 'CloudHSM': 'cloudhsm',
    'CloudSearch': 'cloudsearch',
    'CloudShell': 'cloudshell',
    'CodeDeploy': 'codedeploy',
    'Control Tower': 'control-tower',
    'Customer Enablement': 'customer-enablement',
    'Detective': 'detective',
    'Developer Tools': 'developer-tools',
    'Device Farm': 'device-farm',
    'Directory Service': 'directory-service',
    'EMR': 'emr',
    'Elastic Transcoder': 'elastic-transcoder',
    'Elemental Appliances & Software': 'elemental-appliances-software',
    'End User Computing': 'end-user-computing',
    'Front-end Web & Mobile': 'front-end-web-mobile',
    'Game Development': 'game-development',
    'Ground Station': 'ground-station',
    'Incident Manager': 'incident-manager',
    'Infrastructure Composer': 'infrastructure-composer',
    'Internet of Things': 'internet-of-things',
    'IoT Analytics': 'iot-analytics',
    'IoT Core': 'iot-core',
    'IoT Device Defender': 'iot-device-defender',
    'IoT Device Management': 'iot-device-management',
    'IoT Events': 'iot-events',
    'IoT Greengrass': 'iot-greengrass',
    'IoT SiteWise': 'iot-sitewise',
    'IoT TwinMaker': 'iot-twinmaker',
    'Launch Wizard': 'launch-wizard',
    'MSK': 'msk',
    'Machine Learning': 'machine-learning',
    'Managed Apache Airflow': 'managed-apache-airflow',
    'Managed Apache Flink': 'managed-apache-flink',
    'Managed Services': 'managed-services',
    'Management & Governance': 'management-governance',
    'Media Services': 'media-services',
    'MediaConnect': 'mediaconnect',
    'MediaConvert': 'mediaconvert',
    'MediaLive': 'medialive',
    'MediaPackage': 'mediapackage',
    'MediaStore': 'mediastore',
    'MediaTailor': 'mediatailor',
    'Migration & Transfer': 'migration-transfer',
    'Networking & Content Delivery': 'networking-content-delivery',
    'Oracle Database@AWS': 'oracle-database-aws',
    'Parallel Computing Service': 'parallel-computing-service',
    'Quantum Technologies': 'quantum-technologies',
    'Red Hat OpenShift Service on AWS': 'red-hat-openshift-service',
    'Resource Access Manager': 'resource-access-manager',
    'Resource Groups & Tag Editor': 'resource-groups-tag-editor',
    'Robotics': 'robotics',
    'SWF': 'swf',
    'Satellite': 'satellite',
    'Security Lake': 'security-lake',
    'Service Quotas': 'service-quotas',
    'Support': 'support',
    'Trusted Advisor': 'trusted-advisor',
    'WorkSpaces': 'workspaces',
    'WorkSpaces Secure Browser': 'workspaces-secure-browser',
    'WorkSpaces Thin Client': 'workspaces-thin-client',
    'AWS Deadline Cloud': 'deadline',
    'AWS DeepComposer': 'deepcomposer',
    'AWS End User Messaging': 'pinpoint-sms-voice-v2',
    'AWS Entity Resolution': 'entityresolution',
    'AWS Firewall Manager': 'fms',
    'AWS Health Dashboard': 'health',
    'AWS HealthImaging': 'medical-imaging',
    'AWS HealthLake': 'healthlake',
    'AWS HealthOmics': 'omics',
    'AWS IoT FleetWise': 'iotfleetwise',
    'AWS Lake Formation': 'lakeformation',
    'AWS License Manager': 'license-manager',
    'AWS Mainframe Modernization': 'm2',
    'AWS Marketplace': 'marketplace-catalog',
    'AWS Migration Hub': 'mgh',
    'AWS End User Messaging': 'pinpoint-sms-voice-v2',
    'AWS Entity Resolution': 'entityresolution'
   
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
logger.info("Starting AWS Service Probe for account 721526942678")
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

logger.info("Finished AWS Service Probe for account 721526942678")