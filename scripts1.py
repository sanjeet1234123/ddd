import boto3
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("aws_service_check.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# List of active regions to check. As soon as we find the service in one region,
# we will not check for the other regions.
ACTIVE_REGIONS = ["us-west-2", "us-east-2", "us-east-1", "us-west-1"]

# Path to the input Excel file
INPUT_FILE = "services.xlsx"
# Path for the output Excel file
OUTPUT_FILE = "aws_5g_bedc_dev_services_results1.xlsx"

# Maximum number of retries for AWS API calls
MAX_RETRIES = 3
# Base delay for exponential backoff (in seconds)
BASE_DELAY = 1

def check_aws_credentials():
    """Check for AWS credentials and validate them"""
    required_env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.info("Please set the following environment variables:")
        logger.info("export AWS_ACCESS_KEY_ID='your_access_key'")
        logger.info("export AWS_SECRET_ACCESS_KEY='your_secret_key'")
        if 'AWS_SESSION_TOKEN' in missing_vars:
            logger.info("export AWS_SESSION_TOKEN='your_session_token' (if using temporary credentials)")
        return False
    
    # Verify the credentials work
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        logger.info(f"AWS credentials valid for: {identity['Arn']}")
        return True
    except Exception as e:
        logger.error(f"Error validating AWS credentials: {e}")
        return False

def generate_service_map_from_excel(excel_file):
    """Generate a service map dictionary from the Excel file"""
    try:
        df = pd.read_excel(excel_file)
        # Use the second 'Services' column which contains the actual service names
        services = df.iloc[:, 1].dropna().tolist()  # Get values from second column
        
        # Standard mapping dictionary for known service names to API names
        standard_mapping = {
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
            'Parallel Computing Service': 'parallelcluster',
            'EC2 Global View': 'ec2',
            'Elastic Container Service': 'ecs',
            'Elastic Kubernetes Service': 'eks',
            'Red Hat OpenShift Service on AWS': 'rosa',
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
            'Aurora DSQL': 'rds',
            'Oracle Database@AWS': 'rds',
            'AWS Migration Hub': 'migrationhub',
            'AWS Application Migration Service': 'mgn',
            'Application Discovery Service': 'discovery',
            'Database Migration Service': 'dms',
            'AWS Transfer Family': 'transfer',
            'AWS Snow Family': 'snowball',
            'DataSync': 'datasync',
            'AWS Mainframe Modernization': 'm2',
            'Amazon Elastic VMware Service': 'evs',
            'VPC': 'ec2',  # VPC is part of EC2 API
            'CloudFront': 'cloudfront',
            'API Gateway': 'apigateway',
            'Direct Connect': 'directconnect',
            'AWS App Mesh': 'appmesh',
            'Global Accelerator': 'globalaccelerator',
            'Route 53': 'route53',
            'AWS Data Transfer Terminal': 'datatransfer',
            'AWS Private 5G': 'private-5g',
            'AWS Cloud Map': 'servicediscovery',
            'Application Recovery Controller': 'arc',
            'CodeCommit': 'codecommit',
            'CodeBuild': 'codebuild',
            'CodeDeploy': 'codedeploy',
            'CodePipeline': 'codepipeline',
            'Cloud9': 'cloud9',
            'CloudShell': 'cloudshell',
            'X-Ray': 'xray',
            'AWS FIS': 'fis',
            'Infrastructure Composer': 'infra-composer',
            'AWS App Studio': 'appstudio',
            'AWS AppConfig': 'appconfig',
            'CodeArtifact': 'codeartifact',
            'Amazon CodeCatalyst': 'codecatalyst',
            'Amazon Q Developer': 'q',
            'AWS IQ': 'iq',
            'Managed Services': 'managedservices',
            'Activate for Startups': 'activate',
            'AWS re:Post Private': 'repost',
            'Support': 'support',
            'AWS RoboMaker': 'robomaker',
            'Amazon Managed Blockchain': 'managedblockchain',
            'Ground Station': 'groundstation',
            'Amazon Braket': 'braket',
            'AWS Organizations': 'organizations',
            'CloudWatch': 'cloudwatch',
            'AWS Auto Scaling': 'autoscaling',
            'CloudFormation': 'cloudformation',
            'AWS Config': 'config',
            'OpsWorks': 'opsworks',
            'Service Catalog': 'servicecatalog',
            'Systems Manager': 'ssm',
            'Trusted Advisor': 'trustedadvisor',
            'Control Tower': 'controltower',
            'AWS Well-Architected Tool': 'wellarchitected',
            'Amazon Q Developer in chat applications': 'qdeveloper',
            'Launch Wizard': 'launchwizard',
            'AWS Compute Optimizer': 'compute-optimizer',
            'Resource Groups & Tag Editor': 'resource-groups',
            'Amazon Grafana': 'grafana',
            'Amazon Prometheus': 'prometheus',
            'AWS Resilience Hub': 'resiliencehub',
            'Incident Manager': 'ssm-incidents',
            'AWS Telco Network Builder': 'tnb',
            'AWS Health Dashboard': 'health',
            'AWS Proton': 'proton',
            'AWS User Notifications': 'notifications',
            'CloudTrail': 'cloudtrail',
            'AWS License Manager': 'license-manager',
            'AWS Resource Explorer': 'resource-explorer-2',
            'Service Quotas': 'service-quotas',
            'Kinesis Video Streams': 'kinesisvideo',
            'MediaConvert': 'mediaconvert',
            'MediaLive': 'medialive',
            'MediaPackage': 'mediapackage',
            'MediaStore': 'mediastore',
            'MediaTailor': 'mediatailor',
            'Elemental Appliances & Software': 'elemental',
            'Elastic Transcoder': 'elastictranscoder',
            'Amazon Interactive Video Service': 'ivs',
            'AWS Deadline Cloud': 'deadline',
            'MediaConnect': 'mediaconnect',
            'Amazon SageMaker AI': 'sagemaker',
            'Amazon Augmented AI': 'a2i',
            'Amazon CodeGuru': 'codeguru',
            'Amazon DevOps Guru': 'devops-guru',
            'Amazon Comprehend': 'comprehend',
            'Amazon Forecast': 'forecast',
            'Amazon Fraud Detector': 'frauddetector',
            'Amazon Kendra': 'kendra',
            'Amazon Personalize': 'personalize',
            'Amazon Polly': 'polly',
            'Amazon Rekognition': 'rekognition',
            'Amazon Textract': 'textract',
            'Amazon Transcribe': 'transcribe',
            'Amazon Translate': 'translate',
            'AWS DeepComposer': 'deepcomposer',
            'AWS DeepRacer': 'deepracer',
            'AWS Panorama': 'panorama',
            'Amazon Monitron': 'monitron',
            'AWS HealthLake': 'healthlake',
            'Amazon Lookout for Vision': 'lookoutvision',
            'Amazon Lookout for Equipment': 'lookoutequipment',
            'Amazon Lookout for Metrics': 'lookoutmetrics',
            'Amazon Q Business': 'qbusiness',
            'AWS HealthOmics': 'omics',
            'Amazon Bedrock': 'bedrock',
            'Amazon Q': 'q',
            'Amazon Comprehend Medical': 'comprehendmedical',
            'Amazon Lex': 'lex',
            'AWS HealthImaging': 'medical-imaging',
            'Athena': 'athena',
            'Amazon Redshift': 'redshift',
            'CloudSearch': 'cloudsearch',
            'Amazon OpenSearch Service': 'opensearch',
            'Kinesis': 'kinesis',
            'QuickSight': 'quicksight',
            'AWS Data Exchange': 'dataexchange',
            'AWS Lake Formation': 'lakeformation',
            'MSK': 'kafka',
            'AWS Glue DataBrew': 'databrew',
            'Amazon FinSpace': 'finspace',
            'Managed Apache Flink': 'kinesisanalytics',
            'EMR': 'emr',
            'AWS Clean Rooms': 'cleanrooms',
            'Amazon SageMaker': 'sagemaker',
            'AWS Entity Resolution': 'entityresolution',
            'AWS Glue': 'glue',
            'Amazon Data Firehose': 'firehose',
            'Amazon DataZone': 'datazone',
            'Resource Access Manager': 'ram',
            'Cognito': 'cognito-idp',
            'Secrets Manager': 'secretsmanager',
            'GuardDuty': 'guardduty',
            'Amazon Inspector': 'inspector2',
            'Amazon Macie': 'macie2',
            'IAM Identity Center': 'sso',
            'Certificate Manager': 'acm',
            'Key Management Service': 'kms',
            'CloudHSM': 'cloudhsm',
            'Directory Service': 'ds',
            'AWS Firewall Manager': 'fms',
            'AWS Artifact': 'artifact',
            'Detective': 'detective',
            'AWS Signer': 'signer',
            'Security Lake': 'securitylake',
            'WAF & Shield': 'waf',
            'Amazon Verified Permissions': 'verifiedpermissions',
            'AWS Audit Manager': 'auditmanager',
            'Security Hub': 'securityhub',
            'IAM': 'iam',
            'AWS Private Certificate Authority': 'acm-pca',
            'AWS Payment Cryptography': 'payment-cryptography',
            'AWS Security Incident Response': 'security-incidents',
            'AWS Marketplace': 'marketplace',
            'AWS Billing Conductor': 'billingconductor',
            'Billing and Cost Management': 'ce',
            'AWS Amplify': 'amplify',
            'AWS AppSync': 'appsync',
            'Device Farm': 'devicefarm',
            'Amazon Location Service': 'location',
            'Step Functions': 'stepfunctions',
            'Amazon AppFlow': 'appflow',
            'Amazon MQ': 'mq',
            'Simple Notification Service': 'sns',
            'Simple Queue Service': 'sqs',
            'SWF': 'swf',
            'Managed Apache Airflow': 'mwaa',
            'AWS B2B Data Interchange': 'b2bi',
            'Amazon EventBridge': 'events',
            'Amazon Connect': 'connect',
            'Amazon Chime': 'chime',
            'Amazon Simple Email Service': 'ses',
            'Amazon WorkDocs': 'workdocs',
            'Amazon WorkMail': 'workmail',
            'AWS Supply Chain': 'supplychain',
            'Amazon Pinpoint': 'pinpoint',
            'Amazon One Enterprise': 'one',
            'AWS Wickr': 'wickr',
            'AWS AppFabric': 'appfabric',
            'AWS End User Messaging': 'endusermessaging',
            'Amazon Chime SDK': 'chime-sdk',
            'WorkSpaces': 'workspaces',
            'AppStream 2.0': 'appstream',
            'WorkSpaces Thin Client': 'workspaces-thin-client',
            'WorkSpaces Secure Browser': 'workspaces-web',
            'IoT Analytics': 'iotanalytics',
            'IoT Device Defender': 'iot-device-defender',
            'IoT Device Management': 'iot',
            'IoT Greengrass': 'greengrass',
            'IoT SiteWise': 'iotsitewise',
            'IoT Core': 'iot',
            'IoT TwinMaker': 'iottwinmaker',
            'IoT Events': 'iotevents',
            'AWS IoT FleetWise': 'iotfleetwise',
            'Amazon GameLift Servers': 'gamelift',
            'Amazon GameLift Streams': 'gamelift'
        }
        
        # Create a normalized mapping dictionary (lowercase keys for comparison)
        normalized_mapping = {k.lower(): v for k, v in standard_mapping.items()}
        
        # New map to store the services from Excel with their API names
        result_map = {}
        
        # Map each service from Excel to its API name
        for service in services:
            mapped = False
            service_lower = service.lower()
            
            # Direct match
            if service_lower in normalized_mapping:
                result_map[service] = normalized_mapping[service_lower]
                mapped = True
            else:
                # Try to find partial matches
                for known_service, api_name in normalized_mapping.items():
                    # Check if known service is in the service name or vice versa
                    if known_service in service_lower or service_lower in known_service:
                        result_map[service] = api_name
                        mapped = True
                        break
            
            # If no match found, use a default mapping based on the service name
            if not mapped:
                # Create an API name from the service name (remove spaces, lowercase)
                api_name = service_lower.replace(' ', '').replace('-', '').replace('&', '').replace('@', '')
                result_map[service] = api_name
                logger.warning(f"No mapping found for '{service}'. Using '{api_name}' as default.")
        
        return result_map
    except Exception as e:
        logger.error(f"Error generating service map: {e}")
        return {}

def retry_api_call(func, *args, **kwargs):
    """
    Retry AWS API calls with exponential backoff
    """
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] in ['Throttling', 'ThrottlingException', 'RequestLimitExceeded']:
                if attempt < MAX_RETRIES - 1:  # don't sleep after the last attempt
                    delay = (2 ** attempt) * BASE_DELAY
                    logger.warning(f"API throttling encountered. Retrying in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logger.error(f"Maximum retries reached for API call.")
                    raise
            else:
                # For other types of ClientError, don't retry
                raise
        except Exception as e:
            # For other exceptions, don't retry
            raise

def check_service(client, service_name):
    """
    Generic function to check if a service is being used
    Returns True if the service is in use
    """
    try:
        # Compute Services
        if service_name == 'ec2':
            response = retry_api_call(client.describe_instances)
            return len(response.get('Reservations', [])) > 0
        elif service_name == 'lambda':
            response = retry_api_call(client.list_functions)
            return len(response.get('Functions', [])) > 0
        elif service_name == 'batch':
            response = retry_api_call(client.describe_job_queues)
            return len(response.get('jobQueues', [])) > 0
        elif service_name == 'lightsail':
            response = retry_api_call(client.get_instances)
            return len(response.get('instances', [])) > 0
        elif service_name == 'elasticbeanstalk':
            response = retry_api_call(client.describe_environments)
            return len(response.get('Environments', [])) > 0
        elif service_name == 'serverlessrepo':
            response = retry_api_call(client.list_applications)
            return len(response.get('Applications', [])) > 0
        elif service_name == 'outposts':
            response = retry_api_call(client.list_outposts)
            return len(response.get('Outposts', [])) > 0
        elif service_name == 'imagebuilder':
            response = retry_api_call(client.list_image_pipelines)
            return len(response.get('imagePipelineList', [])) > 0
        elif service_name == 'apprunner':
            response = retry_api_call(client.list_services)
            return len(response.get('ServiceSummaryList', [])) > 0
        elif service_name == 'simspaceweaver':
            response = retry_api_call(client.list_simulations)
            return len(response.get('simulations', [])) > 0
        elif service_name == 'parallelcluster':
            # AWS ParallelCluster doesn't have a direct API, it uses CloudFormation
            cf_client = boto3.client('cloudformation', region_name=client.meta.region_name)
            stacks = retry_api_call(cf_client.list_stacks)
            return any('parallelcluster' in stack.get('StackName', '').lower() for stack in stacks.get('StackSummaries', []))
        
        # Container Services
        elif service_name == 'ecs':
            response = retry_api_call(client.list_clusters)
            return len(response.get('clusterArns', [])) > 0
        elif service_name == 'eks':
            response = retry_api_call(client.list_clusters)
            return len(response.get('clusters', [])) > 0
        elif service_name == 'ecr':
            response = retry_api_call(client.describe_repositories)
            return len(response.get('repositories', [])) > 0
        elif service_name == 'rosa':
            # Red Hat OpenShift Service uses EKS under the hood
            clusters = retry_api_call(client.list_clusters).get('clusters', [])
            return any('openshift' in cluster.lower() for cluster in clusters)
        
        # Storage Services
        elif service_name == 's3':
            response = retry_api_call(client.list_buckets)
            return len(response.get('Buckets', [])) > 0
        elif service_name == 'efs':
            response = retry_api_call(client.describe_file_systems)
            return len(response.get('FileSystems', [])) > 0
        elif service_name == 'fsx':
            response = retry_api_call(client.describe_file_systems)
            return len(response.get('FileSystems', [])) > 0
        elif service_name == 'glacier':
            response = retry_api_call(client.list_vaults)
            return len(response.get('VaultList', [])) > 0
        elif service_name == 'storagegateway':
            response = retry_api_call(client.list_gateways)
            return len(response.get('Gateways', [])) > 0
        elif service_name == 'backup':
            response = retry_api_call(client.list_backup_vaults)
            return len(response.get('BackupVaultList', [])) > 0
        elif service_name == 'drs':
            response = retry_api_call(client.describe_source_servers)
            return len(response.get('items', [])) > 0
        
        # Database Services
        elif service_name == 'rds':
            response = retry_api_call(client.describe_db_instances)
            return len(response.get('DBInstances', [])) > 0
        elif service_name == 'dynamodb':
            response = retry_api_call(client.list_tables)
            return len(response.get('TableNames', [])) > 0
        elif service_name == 'elasticache':
            response = retry_api_call(client.describe_cache_clusters)
            return len(response.get('CacheClusters', [])) > 0
        elif service_name == 'neptune':
            response = retry_api_call(client.describe_db_instances)
            return any(instance['Engine'] == 'neptune' for instance in response.get('DBInstances', []))
        elif service_name == 'docdb':
            response = retry_api_call(client.describe_db_instances)
            return any(instance['Engine'] == 'docdb' for instance in response.get('DBInstances', []))
        elif service_name == 'qldb':
            response = retry_api_call(client.list_ledgers)
            return len(response.get('Ledgers', [])) > 0
        elif service_name == 'keyspaces':
            response = retry_api_call(client.list_keyspaces)
            return len(response.get('keyspaces', [])) > 0
        elif service_name == 'timestream-write':
            response = retry_api_call(client.list_databases)
            return len(response.get('Databases', [])) > 0
        elif service_name == 'memorydb':
            response = retry_api_call(client.list_clusters)
            return len(response.get('Clusters', [])) > 0
        
        # Migration Services
        elif service_name == 'dms':
            response = retry_api_call(client.describe_replication_instances)
            return len(response.get('ReplicationInstances', [])) > 0
        elif service_name == 'datasync':
            response = retry_api_call(client.list_tasks)
            return len(response.get('Tasks', [])) > 0
        elif service_name == 'mgn':
            response = retry_api_call(client.describe_source_servers)
            return len(response.get('items', [])) > 0
        elif service_name == 'transfer':
            response = retry_api_call(client.list_servers)
            return len(response.get('Servers', [])) > 0
        elif service_name == 'snowball':
            response = retry_api_call(client.list_jobs)
            return len(response.get('JobListEntries', [])) > 0
        elif service_name == 'discovery':
            response = retry_api_call(client.describe_agents)
            return len(response.get('agents', [])) > 0
        
        # Networking Services
        elif service_name == 'vpc' and service_name == 'ec2':
            response = retry_api_call(client.describe_vpcs)
            return len(response.get('Vpcs', [])) > 0
        elif service_name == 'apigateway':
            response = retry_api_call(client.get_rest_apis)
            return len(response.get('items', [])) > 0
        elif service_name == 'route53':
            response = retry_api_call(client.list_hosted_zones)
            return len(response.get('HostedZones', [])) > 0
        elif service_name == 'cloudfront':
            response = retry_api_call(client.list_distributions)
            return len(response.get('DistributionList', {}).get('Items', [])) > 0
        elif service_name == 'directconnect':
            response = retry_api_call(client.describe_connections)
            return len(response.get('connections', [])) > 0
        elif service_name == 'globalaccelerator':
            response = retry_api_call(client.list_accelerators)
            return len(response.get('Accelerators', [])) > 0
        elif service_name == 'appmesh':
            response = retry_api_call(client.list_meshes)
            return len(response.get('meshes', [])) > 0
        elif service_name == 'servicediscovery':
            response = retry_api_call(client.list_namespaces)
            return len(response.get('Namespaces', [])) > 0
        
        # Developer Tools
        elif service_name == 'codecommit':
            response = retry_api_call(client.list_repositories)
            return len(response.get('repositories', [])) > 0
        elif service_name == 'codebuild':
            response = retry_api_call(client.list_projects)
            return len(response.get('projects', [])) > 0
        elif service_name == 'codedeploy':
            response = retry_api_call(client.list_applications)
            return len(response.get('applications', [])) > 0
        elif service_name == 'codepipeline':
            response = retry_api_call(client.list_pipelines)
            return len(response.get('pipelines', [])) > 0
        elif service_name == 'cloud9':
            response = retry_api_call(client.list_environments)
            return len(response.get('environmentIds', [])) > 0
        elif service_name == 'xray':
            yesterday = datetime.now() - timedelta(days=1)
            response = retry_api_call(client.get_trace_summaries, StartTime=yesterday, EndTime=datetime.now())
            return len(response.get('TraceSummaries', [])) > 0
        elif service_name == 'fis':
            response = retry_api_call(client.list_experiments)
            return len(response.get('experiments', [])) > 0
        elif service_name == 'codeartifact':
            response = retry_api_call(client.list_domains)
            return len(response.get('domains', [])) > 0
        
        # Management Services
        elif service_name == 'cloudwatch':
            response = retry_api_call(client.list_metrics)
            return len(response.get('Metrics', [])) > 0
        elif service_name == 'cloudformation':
            response = retry_api_call(client.list_stacks)
            return len(response.get('StackSummaries', [])) > 0
        elif service_name == 'cloudtrail':
            response = retry_api_call(client.list_trails)
            return len(response.get('Trails', [])) > 0
        elif service_name == 'config':
            response = retry_api_call(client.describe_config_rules)
            return len(response.get('ConfigRules', [])) > 0
        elif service_name == 'opsworks':
            response = retry_api_call(client.describe_stacks)
            return len(response.get('Stacks', [])) > 0
        elif service_name == 'servicecatalog':
            response = retry_api_call(client.list_portfolios)
            return len(response.get('PortfolioDetails', [])) > 0
        elif service_name == 'ssm':
            response = retry_api_call(client.list_documents)
            return len(response.get('DocumentIdentifiers', [])) > 0
        elif service_name == 'organizations':
            response = retry_api_call(client.list_accounts)
            return len(response.get('Accounts', [])) > 0
        elif service_name == 'health':
            response = retry_api_call(client.describe_events)
            return len(response.get('events', [])) > 0
        elif service_name == 'proton':
            response = retry_api_call(client.list_environments)
            return len(response.get('environments', [])) > 0
        elif service_name == 'grafana':
            response = retry_api_call(client.list_workspaces)
            return len(response.get('workspaces', [])) > 0
        
        # Security Services
        elif service_name == 'iam':
            response = retry_api_call(client.list_users)
            return len(response.get('Users', [])) > 0
        elif service_name == 'kms':
            response = retry_api_call(client.list_keys)
            return len(response.get('Keys', [])) > 0
        elif service_name == 'secretsmanager':
            response = retry_api_call(client.list_secrets)
            return len(response.get('SecretList', [])) > 0
        elif service_name == 'cognito-idp':
            response = retry_api_call(client.list_user_pools)
            return len(response.get('UserPools', [])) > 0
        elif service_name == 'guardduty':
            response = retry_api_call(client.list_detectors)
            return len(response.get('DetectorIds', [])) > 0
        elif service_name == 'inspector2':
            response = retry_api_call(client.list_findings)
            return len(response.get('findings', [])) > 0
        elif service_name == 'macie2':
            response = retry_api_call(client.list_classifications_jobs)
            return len(response.get('items', [])) > 0
        elif service_name == 'sso':
            response = retry_api_call(client.list_instances)
            return len(response.get('Instances', [])) > 0
        elif service_name == 'acm':
            response = retry_api_call(client.list_certificates)
            return len(response.get('CertificateSummaryList', [])) > 0
        elif service_name == 'waf':
            response = retry_api_call(client.list_web_acls)
            return len(response.get('WebACLs', [])) > 0
        elif service_name == 'shield':
            response = retry_api_call(client.list_protections)
            return len(response.get('Protections', [])) > 0
        elif service_name == 'securityhub':
            response = retry_api_call(client.list_findings)
            return len(response.get('Findings', [])) > 0
        elif service_name == 'sns':
            response = retry_api_call(client.list_topics)
            return len(response.get('Topics', [])) > 0
        elif service_name == 'sqs':
            response = retry_api_call(client.list_queues)
            return len(response.get('QueueUrls', [])) > 0
        elif service_name == 'events':
            response = retry_api_call(client.list_rules)
            return len(response.get('Rules', [])) > 0
        elif service_name == 'stepfunctions':
            response = retry_api_call(client.list_state_machines)
            return len(response.get('stateMachines', [])) > 0
        elif service_name == 'mq':
            response = retry_api_call(client.list_brokers)
            return len(response.get('BrokerSummaries', [])) > 0
        elif service_name == 'athena':
            response = retry_api_call(client.list_work_groups)
            return len(response.get('WorkGroups', [])) > 0
        elif service_name == 'redshift':
            response = retry_api_call(client.describe_clusters)
            return len(response.get('Clusters', [])) > 0
        elif service_name == 'opensearch':
            response = retry_api_call(client.list_domain_names)
            return len(response.get('DomainNames', [])) > 0
        elif service_name == 'kinesis':
            response = retry_api_call(client.list_streams)
            return len(response.get('StreamNames', [])) > 0
        elif service_name == 'quicksight':
            response = retry_api_call(client.list_users)
            return len(response.get('UserList', [])) > 0
        elif service_name == 'glue':
            response = retry_api_call(client.get_databases)
            return len(response.get('DatabaseList', [])) > 0
        elif service_name == 'firehose':
            response = retry_api_call(client.list_delivery_streams)
            return len(response.get('DeliveryStreamNames', [])) > 0
        
        return False
    except Exception as e:
        logger.error(f"Error checking {service_name}: {str(e)}")
        return False

def check_services(client):
    """
    Check all services for a given client
    """
    service_map = generate_service_map_from_excel(INPUT_FILE)
    results = {}
    for service, api_name in service_map.items():
        results[service] = check_service(client, api_name)
    return results

def main():
    if not check_aws_credentials():
        return

    # Initialize results dictionary
    all_results = {}
    
    # Check services in each region
    for region in ACTIVE_REGIONS:
        logger.info(f"Checking services in region: {region}")
        try:
            # Create clients for different services
            ec2_client = boto3.client('ec2', region_name=region)
            s3_client = boto3.client('s3', region_name=region)
            lambda_client = boto3.client('lambda', region_name=region)
            rds_client = boto3.client('rds', region_name=region)
            dynamodb_client = boto3.client('dynamodb', region_name=region)
            cloudwatch_client = boto3.client('cloudwatch', region_name=region)
            iam_client = boto3.client('iam', region_name=region)
            apigateway_client = boto3.client('apigateway', region_name=region)
            cloudfront_client = boto3.client('cloudfront', region_name=region)
            route53_client = boto3.client('route53', region_name=region)
            codecommit_client = boto3.client('codecommit', region_name=region)
            cloudtrail_client = boto3.client('cloudtrail', region_name=region)
            
            # Create a dictionary of clients for different services
            clients = {
                'ec2': ec2_client,
                's3': s3_client,
                'lambda': lambda_client,
                'rds': rds_client,
                'dynamodb': dynamodb_client,
                'cloudwatch': cloudwatch_client,
                'iam': iam_client,
                'apigateway': apigateway_client,
                'cloudfront': cloudfront_client,
                'route53': route53_client,
                'codecommit': codecommit_client,
                'cloudtrail': cloudtrail_client
            }
            
            # Get service map from Excel
            service_map = generate_service_map_from_excel(INPUT_FILE)
            
            # Check each service
            for service, api_name in service_map.items():
                if service not in all_results:
                    all_results[service] = {'In Use': False, 'Region': 'N/A'}
                
                # Skip if service is already found in use
                if all_results[service]['In Use']:
                    continue
                
                # Get appropriate client for the service
                client = clients.get(api_name)
                if client:
                    try:
                        is_in_use = check_service(client, api_name)
                        if is_in_use:
                            all_results[service] = {
                                'In Use': True,
                                'Region': region
                            }
                            logger.info(f"Service {service} found in use in region {region}")
                    except Exception as e:
                        logger.error(f"Error checking {service} in region {region}: {str(e)}")
                else:
                    logger.warning(f"No client available for service {service} ({api_name})")
                    
        except Exception as e:
            logger.error(f"Error processing region {region}: {str(e)}")
            continue

    # Convert results to DataFrame
    results_list = []
    for service, details in all_results.items():
        results_list.append({
            'Service': service,
            'Status': 'In Use' if details['In Use'] else 'Not In Use',
            'Region': details['Region'],
            'Last Checked': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    df = pd.DataFrame(results_list)
    
    # Save results to Excel
    df.to_excel(OUTPUT_FILE, index=False)
    logger.info(f"Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()