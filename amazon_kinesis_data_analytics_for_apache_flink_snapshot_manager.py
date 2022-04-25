# Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import time
import json
import boto3
import logging
import datetime
import botocore

# setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    AWS Lambda function's handler function. It takes a snapshot of a Kinesis Data Analytics Flink application,
    retains the most recent X number of snapshots, and deletes the rest. For X, see parameter
    'num_of_older_snapshots_to_retain'. :param event: :param context: :return:
    """
    print('Running Snapshot Manager. Input event:', json.dumps(event, indent=4))
    # read environment variables
    region = os.environ['aws_region']
    flink_app_name = os.environ['app_name']
    ddb_table_name = os.environ['snapshot_manager_ddb_table_name']
    primary_partition_key_name = os.environ['primary_partition_key_name']
    primary_sort_key_name = os.environ['primary_sort_key_name']
    sns_topic_arn = os.environ['sns_topic_arn']
    num_of_older_snapshots_to_retain = int(os.environ['number_of_older_snapshots_to_retain'])
    snapshot_creation_wait_time_seconds = int(os.environ['snapshot_creation_wait_time_seconds'])

    # setup clients
    sns = boto3.client('sns', region)
    dynamodb = boto3.client('dynamodb', region)
    kinesis_analytics = boto3.client('kinesisanalyticsv2', region)

    # initialize variables
    deleted_snapshots = []
    not_deleted_snapshots = []
    snapshot_deletion_status = {
        "deleted_snapshots":  deleted_snapshots,
        "not_deleted_snapshots": not_deleted_snapshots
    }
    snapshot_manager_run_id = int(round(time.time() * 1000))
    snapshot_name = 'custom_' + str(snapshot_manager_run_id)
    response_body = {
        "app_name": flink_app_name,
        "app_version": "",
        "snapshot_manager_run_id": snapshot_manager_run_id,
        "new_snapshot_name": snapshot_name,
        "app_is_running": True,
        "app_is_healthy": True,
        "new_snapshot_initiated": False,
        "new_snapshot_completed": False,
        "new_snapshot_creation_delayed": False,
        "old_snapshots_to_be_deleted": False,
        "num_of_snapshot_deleted": 0,
        "num_of_snapshot_not_deleted": 0
    }
    print('Snapshot Manager Execution Status. Run Id: {0}'.format(snapshot_manager_run_id))

    # describe application to get application status and current version
    response = describe_flink_application(kinesis_analytics, flink_app_name)
    response_body['app_version'] = response['ApplicationDetail']['ApplicationVersionId']

    # If application is running then takes a snapshot
    if response['ApplicationDetail']['ApplicationStatus'] == 'RUNNING':
        snapshot_creation_res = take_app_snapshot(kinesis_analytics, flink_app_name, snapshot_name)
        if snapshot_creation_res['is_initiated']:
            response_body['new_snapshot_initiated'] = True
        else:
            response_body['app_is_healthy'] = False
    else:
        response_body['app_is_running'] = False
        # error_message = 'A new snapshot cannot be taken. Flink application {0} is not running.'.format(flink_app_name)
        error_message = """
                    Application Team:

                    Snapshot Manager execution completed. Run Id: {0}. However, a new snapshot has not been taken.
                    The application {1} is not running.
                    """.format(snapshot_manager_run_id, flink_app_name)

        print(error_message)
        notify_error(sns, sns_topic_arn, error_message)

    # If application is not healthy then send a notification
    if not response_body['app_is_healthy']:
        error_message = """
                            Application Team:

                            Snapshot Manager execution completed. Run Id: {0}. However, a new snapshot has not been 
                            taken. The application {1} may not be healthy.""".format(snapshot_manager_run_id, flink_app_name)

        print(error_message)
        notify_error(sns, sns_topic_arn, error_message)

    # If new snapshot creation initiated then check if it is completed
    max_checks = 4
    checks_done = 0
    if response_body['new_snapshot_initiated']:
        while checks_done < max_checks:
            time.sleep(snapshot_creation_wait_time_seconds)
            snapshots = list_flink_app_snapshots(kinesis_analytics, flink_app_name, response_body['app_version'])
            print('Application {0} of version {1} has {2} snapshots: '.format(flink_app_name,
                                                                              response_body['app_version'],
                                                                              len(snapshots)))
            sorted_snapshots = sorted(snapshots, key=lambda k: k['SnapshotCreationTimestamp'], reverse=True)
            latest_snapshot = sorted_snapshots[0]
            if latest_snapshot['SnapshotName'] == snapshot_creation_res['snapshot_name']:
                if latest_snapshot['SnapshotStatus'] == 'READY':
                    checks_done = 4
                    response_body['new_snapshot_completed'] = True
                    print(response_body)
                    send_sns_notification(sns, sns_topic_arn, flink_app_name, snapshot_manager_run_id, snapshot_name,
                                          latest_snapshot, True)
                else:
                    checks_done += 1
            else:
                print('No snapshot found with the name: {0}'.format(snapshot_creation_res['snapshot_name']))

    if checks_done == 4 and not response_body['new_snapshot_completed']:
        print("Snapshot creation has been delayed")
        response_body['new_snapshot_creation_delayed'] = True

    # If newly initiated snapshot is not completed on time then send a notification
    if response_body['new_snapshot_creation_delayed']:
        send_sns_notification(sns, sns_topic_arn, flink_app_name, snapshot_manager_run_id, snapshot_name, None, False)
    
    if response_body['new_snapshot_completed']:
        num_of_snapshots_after_new_snapshot = list_flink_app_snapshots(kinesis_analytics, flink_app_name,
                                                                       response['ApplicationDetail'][
                                                                           'ApplicationVersionId'])
        # check if the number of old snapshots exceeds the threshold.
        if len(num_of_snapshots_after_new_snapshot) > num_of_older_snapshots_to_retain:
            response_body['old_snapshots_to_be_deleted'] = True

    # initiate old snapshot deletion process
    if response_body['old_snapshots_to_be_deleted']:
        sorted_snapshots_after_new_snapshots = sorted(num_of_snapshots_after_new_snapshot,
                                                      key=lambda k: k['SnapshotCreationTimestamp'], reverse=True)
        snapshots_to_be_deleted = sorted_snapshots_after_new_snapshots[num_of_older_snapshots_to_retain:None]
        for snapshot_to_be_deleted in snapshots_to_be_deleted:
            snapshot_deleted = delete_snapshot(kinesis_analytics, flink_app_name, snapshot_to_be_deleted)
            if snapshot_deleted:
                deleted_snapshots.append(snapshot_to_be_deleted)
                print(
                    'Snapshot deleted: {0}, name: {1}'.format(snapshot_deleted,
                                                              snapshot_to_be_deleted['SnapshotName']))
            else:
                not_deleted_snapshots.append(snapshot_to_be_deleted)
        # add deleted and not-deleted snapshots to snapshot_deletion_status dictionary
        snapshot_deletion_status['deleted_snapshots'] = deleted_snapshots
        snapshot_deletion_status['not_deleted_snapshots'] = not_deleted_snapshots
        response_body['num_of_snapshot_deleted'] = len(deleted_snapshots)
        response_body['num_of_snapshot_not_deleted'] = len(not_deleted_snapshots)
    else:
        logger.info('Number of historical snapshots less than the threshold. No need to delete any snapshots.')

    # Tracking and Notifications
    if response_body['new_snapshot_completed']:
        track_snapshot_manager_status(dynamodb, ddb_table_name, primary_partition_key_name, primary_sort_key_name,
                                      flink_app_name, snapshot_manager_run_id, latest_snapshot,
                                      snapshot_deletion_status)

    return_response = {'statusCode': 200, 'body': json.dumps(response_body)}
    return return_response


def describe_flink_application(kin_analytics, flink_app_name):
    """
    This function describes a Kinesis Data Analytics Flink Application
    :param kin_analytics:
    :param flink_app_name:
    :return:
    """
    try:
        res = kin_analytics.describe_application(ApplicationName=flink_app_name, IncludeAdditionalDetails=True)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested Kinesis Data Analytics Flink Application was not found')
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))
    return res


def list_flink_app_snapshots(kin_analytics, flink_app_name, app_ver_id):
    """
    This function get a list of snapshots for a Kinesis Data Analytics Flink Application
    :param kin_analytics:
    :param flink_app_name:
    :param app_ver_id:
    :return:
    """
    app_snapshots_latest_version = []
    try:
        response = kin_analytics.list_application_snapshots(ApplicationName=flink_app_name, Limit=10)
        snapshot_summary_list = response['SnapshotSummaries']
        for snapshot_summary in snapshot_summary_list:
            if app_ver_id == snapshot_summary['ApplicationVersionId']:
                app_snapshots_latest_version.append(snapshot_summary)
        # process next set list of items if 'NextToken' exist in the response
        while 'NextToken' in response:
            response = kin_analytics.list_application_snapshots(
                ApplicationName=flink_app_name, Limit=50, NextToken=response['NextToken']
            )
            snapshot_summary_list = response['SnapshotSummaries']
            for snapshot_summary in snapshot_summary_list:
                if app_ver_id == snapshot_summary['ApplicationVersionId']:
                    app_snapshots_latest_version.append(snapshot_summary)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested Kinesis Data Analytics Flink Application was not found')
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))

    return app_snapshots_latest_version


def take_app_snapshot(kin_analytics, flink_app_name, snapshot_name):
    """
    This function takes a Flink snapshot
    :param kin_analytics:
    :param flink_app_name:
    :param snapshot_name:
    :return:
    """
    snapshot_creation_resp = {
        "app_name": flink_app_name,
        "snapshot_name": "",
        "is_initiated": False,
        "error_message": "",
        "app_version": ""
    }
    try:
        res = kin_analytics.create_application_snapshot(ApplicationName=flink_app_name, SnapshotName=snapshot_name)
        if res['ResponseMetadata']['HTTPStatusCode'] == 200:
            snapshot_creation_resp['is_initiated'] = True
            snapshot_creation_resp['snapshot_name'] = snapshot_name
            logger.info('Snapshot creation initiated.')
    except botocore.exceptions.ClientError as error:
        snapshot_creation_resp['error_message'] = error.response['Error']['Message']
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested Kinesis Data Analytics Flink Application was not found')
        elif error.response['Error']['Code'] == 'InvalidRequestException':
            print('Error Message: {}'.format(error.response['Error']['Message']))
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))
    return snapshot_creation_resp


def delete_snapshot(kin_analytics, flink_app_name, snapshot):
    """
    This function deletes a Flink snapshot
    :param kin_analytics:
    :param flink_app_name:
    :param snapshot:
    :return:
    """
    is_snapshot_deleted = False
    try:
        res = kin_analytics.delete_application_snapshot(
            ApplicationName=flink_app_name,
            SnapshotName=snapshot['SnapshotName'],
            SnapshotCreationTimestamp=snapshot['SnapshotCreationTimestamp']
        )
        if res['ResponseMetadata']['HTTPStatusCode'] == 200:
            is_snapshot_deleted = True
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested Kinesis Data Analytics Flink Application was not found')
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))
    return is_snapshot_deleted


def notify_error(sns, topic_arn, message):
    """
    This function sends a notification to Amazon SNS Topic
    :param sns:
    :param topic_arn:
    :param message:
    :return:
    """
    try:
        pub_response = sns.publish(TopicArn=topic_arn, Message=message,
                                   Subject='Kinesis Data Analytics Flink Snapshot Manager Alert')
        if pub_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            message_sent = True
            logger.info(
                'Message published to SNS Topic successfully. Message Id: {0}'.format(pub_response['MessageId']))
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested SNS Topic was not found')
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))
    return message_sent


def send_sns_notification(sns, topic_arn, flink_app_name, snapshot_manager_run_id, snapshot_name, new_snapshot: None,
                          snapshot_created):
    """
    This function sends a notification to Amazon SNS Topic
    :param snapshot_name:
    :param sns:
    :param topic_arn:
    :param flink_app_name:
    :param snapshot_manager_run_id:
    :param new_snapshot:
    :param snapshot_created:
    :return:
    """
    message_sent = False
    if snapshot_created:
        message = """
        Application Team:
        
        Snapshot Manager execution completed. Run Id: {0}.
        
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        
        New snapshot creation details:   
             - Application Name: {1}
             - Snapshot name: {2}
             - Application version Id: {3}
             - Snapshot Creation Time: {4}
         
        Historical snapshot(s) deletion status:         
             - Refer DynamoDB audit table for details. primary partition key: {5}, primary sort key: {6}.
        
        ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    """.format(snapshot_manager_run_id, flink_app_name, new_snapshot['SnapshotName'],
               new_snapshot['ApplicationVersionId'],
               new_snapshot['SnapshotCreationTimestamp'], flink_app_name, snapshot_manager_run_id)
    else:
        message = """
                Application Team:
        
                Snapshot Manager execution completed. Run Id: {0}. However, the snapshot creation process either 
                not completed on time or failed. Please investigate Cloudwatch logs and check the Snapshots 
                section under Amazon Kinesis - Analytics applications - Flink Application of your AWS environment. 
                Below are the details:
                
                ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                
                    - Application Name: {1}
                    - Snapshot Name: {2}
                    - Snapshot creation attempted at: {3}
                    
                ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
            """.format(snapshot_manager_run_id, flink_app_name, snapshot_name, datetime.datetime.now())
    try:
        pub_response = sns.publish(TopicArn=topic_arn, Message=message,
                                   Subject='Kinesis Data Analytics Flink Snapshot Manager Alert')
        if pub_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            message_sent = True
            logger.info(
                'Message published to SNS Topic successfully. Message Id: {0}'.format(pub_response['MessageId']))
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested SNS Topic was not found')
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))
    return message_sent


def track_snapshot_manager_status(dynamodb, ddb_table_name, primary_partition_key, primary_sort_key, app_name,
                                  snapshot_manager_run_id, new_snapshot, snapshot_deletion_status):
    """
    This function tracks the status of Snapshot Manager
    :param dynamodb:
    :param ddb_table_name:
    :param primary_partition_key:
    :param primary_sort_key:
    :param app_name:
    :param snapshot_manager_run_id:
    :param new_snapshot:
    :param snapshot_deletion_status:
    :return:
    """
    item_inserted = False
    try:
        # Prepare an item
        item = {
            primary_partition_key: {'S': app_name},
            primary_sort_key: {'N': str(snapshot_manager_run_id)},
            'new_snapshot_name': {'S': str(new_snapshot['SnapshotName'])},
            'new_snapshot_create_time': {'S': str(new_snapshot['SnapshotCreationTimestamp'])},
            'flink_app_version_id': {'S': str(new_snapshot['ApplicationVersionId'])}
        }
        if len(snapshot_deletion_status['deleted_snapshots']) > 0:
            item['snapshots_deleted'] = {'S': str(snapshot_deletion_status['deleted_snapshots'])}
        if len(snapshot_deletion_status['not_deleted_snapshots']) > 0:
            item['snapshots_failed_to_be_deleted'] = {'S': str(snapshot_deletion_status['not_deleted_snapshots'])}
        # Insert the item
        put_item_response = dynamodb.put_item(TableName=ddb_table_name, Item=item)
        if put_item_response['ResponseMetadata']['HTTPStatusCode'] == 200:
            item_inserted = True
            logger.info('An item inserted successfully')
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.warning('The requested DynamoDB table was not found')
        else:
            print('Error Message: {}'.format(error.response['Error']['Message']))
    return item_inserted
