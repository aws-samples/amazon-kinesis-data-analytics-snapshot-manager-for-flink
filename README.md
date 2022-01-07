# Snapshot Manager - Amazon Kinesis Data Analytics for Apache Flink

Snapshot Manager - [Amazon Kinesis Data Analytics](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-it-works.html) for Apache Flink offers the following benefits:

   1. takes a new snapshot of a running Kinesis Data Analytics for Apache Flink  Application
   1. gets a count of application snapshots
   1. checks if the count is more than the required number of snapshots
   1. deletes older snapshots that are older than the required number

This will be deployed as an [AWS Lambda](https://aws.amazon.com/lambda/) function and scheduled using [Amazon CloudWatch Events](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/WhatIsCloudWatchEvents.html) e.g. once in a day or week.

---

## Architecture

![Alt](./amazon_kinesis_data_analytics_for_apache_flink_snapshot_manager.png)

---

## Process Flow Diagram

![Alt](./amazon_kinesis_data_analytics_for_apache_flink_snapshot_manager_flow_diagram.png)

---

## Pre-requisites

  1. Python 3.7
  1. IDE e.g. [PyCharm](https://www.jetbrains.com/pycharm/)
  1. Access to AWS Account
  1. A running Kinesis Data Analytics for Apache Flink Application

---

## AWS Service Requirements

The following AWS services are required to deploy this starter kit:

 1. 1 AWS Lambda Function
 1. 1 Amazon SNS Topic
 1. 1 Amazon DynamoDB Table
 1. 1 IAM role with 4 policies
 1. 1 AWS CloudWatch Event Rule

---

## Deployment Instructions using AWS Console

1. Create an SNS Topic and subscribe required e-mail id(s)
1. Create a DynamoDB Table
   1. Table name= ```snapshot_manager_status```
   1. Primary partition key: name= ```app_name```, type= String
   1. Primary sort key: name= ```snapshot_manager_run_id```, type= Number
   1. Provisioned read capacity units = 5
   1. Provisioned write capacity units = 5
1. Create following IAM policies
   1. IAM policy with name ```iam_policy_dynamodb``` using [this sample](./resources/iam_policy_dynamodb.json)
   1. IAM policy with name ```iam_policy_sns``` using [this sample](./resources/iam_policy_sns.json)
   1. IAM policy with name ```iam_policy_kinesisanalytics``` using [this sample](./resources/iam_policy_kinesisanalytics.json)
   1. IAM policy with name ```iam_policy_cloudwatch_logs``` using [this sample](./resources/iam_policy_cloudwatch_logs.json)
1. Create an IAM role for Lambda with name ```snapshot_manager_iam_role``` and attach above policies
1. Deploy **snapshot_manager** function

    1. Function name = snapshot_manager
    1. Runtime = Python 3.7
    1. IAM role = snapshot_manager_iam_role
    1. Function code = Copy the contents from [amazon_kinesis_data_analytics_for_apache_flink_snapshot_manager.py](./amazon_kinesis_data_analytics_for_apache_flink_snapshot_manager.py)
    1. Under General configuration:
        1. Timeout = e.g. 5 minutes
        1. Memory = e.g. 128 MB
    1. Environment variable = as defined in the following table

         | Key   | Value  | Description |
         |-------| -------| ----------- |
         | aws_region  | us-east-1 | AWS region |
         | app_name | ```Application Name``` | Application name of Kinesis Data Analytics for Apache Flink |
         | snapshot_manager_ddb_table_name  | ```snapshot_manager_status``` | Name of the DynamoDB table used to track the status |
         | primary_partition_key_name | ```app_name``` | Primary partition key name |
         | primary_sort_key_name | ```snapshot_manager_run_id``` | Primary sort key name |
         | sns_topic_arn | ```SNS Topic ARN``` | SNS Topic ARN  |
         | number_of_older_snapshots_to_retain | ```30``` | The number of most recent snapshots to be retained  |
         | snapshot_creation_wait_time_seconds | ```15``` | Time gap in seconds between consecutive checks to get the status of snapshot creation  |
1. Create ```snapshot_manager_rule``` EventBridge rule

    1. Rule name = ```SnapshotManagerEventRule```
    1. Pattern = ```Schedule``` with desired fixed rate e.g. 6 Hours 
    1. Target = Previously created lambda Function ```snapshot_manager```
 

---

## License Summary

This sample code is made available under the MIT-0 license. See the LICENSE file.
