# Bulk registration with boto3 of many iot devices
import boto3
import json
import time
import os
import sys

REGION = 'eu-central-1'
THING_TYPE = 'TRACKER'
THING_NAME_PREFIX = 'MOTO'
ROLE_ARN = 'arn:aws:iam::688793167504:role/JITPRole'
PROVISION_TEMPLATE_FILE = 'provisioning-template.json'
PROVISION_FILE_NAME = 'provisioning-template.json'
PROVISIONNING_DATA_FILE = 'provisioning-data.json'
BUCKET_NAME = 'iot-bulk-provisioning-2'
POLICY_NAME = 'SaasIotPolicy'
# Define max item sizes for search pages
pageSize = 2

class AWSIoTThing():
    """
    This is a generic class for creating things to be used for creating the provisioning file
    """
    count = 0

    def __init__(self, THING_NAME_PREFIX, THING_TYPE_NAME):
        self.thing_type_name = THING_TYPE_NAME
        self.thing_name_prefix = THING_NAME_PREFIX
        self.name = self.thing_name_prefix+"_"+str(AWSIoTThing.count)
        self.id = AWSIoTThing.count
        AWSIoTThing.count += 1



def create_provision_data_file(file_name, number_of_things):
    # Create things
    things = [None]*number_of_things
    for i in range(number_of_things):
        things[i] = AWSIoTThing(THING_NAME_PREFIX, THING_TYPE)

    # Clear the provisioning json file by simply opening for writing
    bulk_provision_file = file_name
    f = open(bulk_provision_file, "w")
    f.close()

    # Reopen the provision data file to attend lines
    f = open(bulk_provision_file, "a")

    # Loop through things and create a provision data for each thing
    for thing in things:
        message = {"ThingName": thing.name,
                   "ThingTypeName": thing.thing_type_name, "ThingId": thing.id}
        json.dump(message, f)
        f.write("\n")

    # Close the file after operation
    f.close()

def create_s3_bucket(bucket_name):
    s3_client = boto3.client('s3', region_name=REGION)
    print('Listing buckets...')
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    print("Existing buckets: %s" % buckets)
    if bucket_name not in buckets:
        print('Creating bucket %s...' % bucket_name)
        s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': REGION})
    else:
        print('Bucket %s already exists' % bucket_name)
        # Erase everything in the bucket
        print('Erasing bucket %s...' % bucket_name)
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

        # Delete the bucket if it already exists and recall the function
        s3_client.delete_bucket(Bucket=bucket_name)
        create_s3_bucket(bucket_name)


def upload_provision_to_s3(bucket_name, file_name):
    s3_client = boto3.client('s3', region_name=REGION)
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    if bucket_name in buckets:
        print('Uploading %s to bucket %s...' % (file_name, bucket_name))
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=open(file_name, 'rb'))
    else:
        print('Bucket %s does not exist' % bucket_name)
    

def aws_iot_core_create_bulk_things(bucket_name):
    iot_client = boto3.client('iot', region_name=REGION)
    print('Creating bulk things...')

    # Step 0: Create a thing type prior to start thing registiration
    response = iot_client.create_thing_type(thingTypeName=THING_TYPE)
    print(response)

    f = open(PROVISION_TEMPLATE_FILE, 'r')

    # Step 1: Start tings registration task
    response = iot_client.start_thing_registration_task(
        templateBody=f.read(),
        inputFileBucket=BUCKET_NAME,
        inputFileKey=PROVISIONNING_DATA_FILE,
        roleArn=ROLE_ARN
    )

    print(response)

    # Step 2: Wait for the task to complete
    task_id = response['taskId']
    while True:
        response = iot_client.describe_thing_registration_task(taskId=task_id)
        print(response)
        if response['status'] == 'Completed':
            break
        elif response['status'] == 'Failed':
            print('Task failed')
            sys.exit(1)
        elif response['status'] == 'InProgress':
            print('Task in progress')
        else:
            print('Unknown status')
            sys.exit(1)
        time.sleep(5)


def create_certificate():
    iot_client = boto3.client('iot', region_name=REGION)
    # Creating a certificate for each thing

    # Step 1: Create the folder for the certificates if it does not exist
    if not os.path.exists('secure'):
        os.makedirs('secure')
    if not os.path.exists('secure/certs'):
        os.makedirs('secure/certs')
    if not os.path.exists('secure/private'):
        os.makedirs('secure/private')
    if not os.path.exists('secure/public'):
        os.makedirs('secure/public')

    things = aws_iot_core_get_all_things()
    for thing in things['thingNames']:
        # Create keys and certificates
        response = iot_client.create_keys_and_certificate(setAsActive=True)

        # Get the certificate and key contents
        certificateArn = response["certificateArn"]
        certificate = response["certificatePem"]
        key_public = response["keyPair"]["PublicKey"]
        key_private = response["keyPair"]["PrivateKey"]

        # Write the certificate and key contents to files
        with open(f"secure/certs/{thing}.pem", "w") as f:
            f.write(certificate)
        with open(f"secure/private/{thing}.pem", "w") as f:
            f.write(key_private)
        with open(f"secure/public/{thing}.pem", "w") as f:
            f.write(key_public)
        



def aws_iot_core_get_all_things():
    """
    returns all the things registered in the aws-iot-core
    """

    # Return parameters
    thingNames = []
    thingArns = []

    # Create client
    iot_client = boto3.client('iot', REGION)

    # Parameters used to count things and search pages
    things_count = 0
    page_count = 0



    # Send the first request
    response = iot_client.list_things(maxResults=pageSize)

    # Count the number of the things until no more things are present on the search pages
    while(1):
        # Increment thing count
        things_count = things_count + len(response['things'])

        # Append found things to the lists
        for thing in response['things']:
            thingArns.append(thing['thingArn'])
            thingNames.append(thing['thingName'])

        # Increment Page number
        page_count += 1

        # Check if nextToken is present for next search pages
        if("nextToken" in response):
            response = iot_client.list_things(
                maxResults=pageSize, nextToken=response["nextToken"])
        else:
            break

    return {"thingArns": thingArns, "thingNames": thingNames}

def aws_iot_core_get_all_certificates():
    """
    returns all the certificates registered in the aws-iot-core
    """

    # Return parameters
    certificateArns = []
    certificateIds = []

    # Create client
    iot_client = boto3.client('iot', REGION)

    # Parameter used to count certificates and search pages
    certificates_count = 0
    page_count = 0

    # Send the first request
    response = iot_client.list_certificates(pageSize=pageSize)

    # Count the number of the certificates until no more certificates are present on the search pages
    while(1):
        # Increment certificate count
        certificates_count = certificates_count + len(response['certificates'])

        # Append found certificates to the lists
        for certificate in response['certificates']:
            certificateArns.append(certificate['certificateArn'])
            certificateIds.append(certificate['certificateId'])


        # Increment Page number
        page_count += 1

        # Check if nextMarker is present for next search pages
        if("nextMarker" in response):
            response = iot_client.list_certificates(
                pageSize=pageSize, marker=response["nextMarker"])
        else:
            break
    return {"certificateArns": certificateArns, "certificateIds": certificateIds}

def aws_iot_core_get_all_policies(detail=False):
    """
    returns all the policies registerd in the aws iot core
    """

    # Return parameters
    policyArns = []
    policyNames = []

    # Parameter used to count policies
    policy_count = 0

    # Create client
    iot_client = boto3.client('iot', REGION)

    # Parameters used to count policies and search pages
    policies_count = 0
    page_count = 0


    # Send the first request
    response = iot_client.list_policies(pageSize=pageSize)

    # Count the number of the things until no more things are present on the search pages
    while(1):
        # Increment policy count
        policies_count = policies_count + len(response['policies'])
        # Append found policies to the lists
        for policy in response['policies']:
            policyArns.append(policy['policyArn'])
            policyNames.append(policy['policyName'])

        # Increment Page number
        page_count += 1

        # Check if nextMarker is present for next search pages
        if("nextMarker" in response):
            response = iot_client.list_policies(
                pageSize=pageSize, Marker=response["nextMarker"])
        else:
            break

    return {"policyArns": policyArns, "policyNames": policyNames}



def aws_iot_core_attach_certificates():
    """
    Attach certificates the things and the policy
    """

    # Create client
    iot_client = boto3.client('iot', REGION)


    thingNames = aws_iot_core_get_all_things()["thingNames"]
    certificateArns = aws_iot_core_get_all_certificates()["certificateArns"]
    policyNames = aws_iot_core_get_all_policies()["policyNames"]

    # Find policy name named POLICY_NAME
    policyName = None
    for policy in policyNames:
        if(policy == POLICY_NAME):
            policyName = policy
            break


    # Attach unique certificates to things and policy to certificates
    if(len(thingNames) == len(certificateArns)):
        for i in range(len(thingNames)):
            # Attach certificate to things
            iot_client.attach_thing_principal(
                thingName=thingNames[i], principal=certificateArns[i])

            # Attach policy to things
            iot_client.attach_principal_policy(
                policyName=policyName, principal=certificateArns[i])
    else:
        print("Number of things and certificates are not equal")

 
    



if __name__ == '__main__':
    # Create the provisioning data file
    create_provision_data_file(PROVISIONNING_DATA_FILE, 5)

    create_s3_bucket(BUCKET_NAME)

    upload_provision_to_s3(BUCKET_NAME, PROVISIONNING_DATA_FILE)

    aws_iot_core_create_bulk_things(BUCKET_NAME)

    create_certificate()

    aws_iot_core_attach_certificates()




    print('Done')


