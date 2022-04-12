#!/usr/bin/env python3
import os

from pstats import SortKey
from aws_cdk import (
    # core as cdk,
    App,
    Stack,
    RemovalPolicy,
    Duration,
    CfnOutput,
    CfnParameter,
    RemovalPolicy,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_lambda as _lambda,
    aws_dynamodb as _dyn,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as events_target
    # aws_sqs as sqs,
)
from constructs import Construct
# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
# from aws_cdk import core


class CdkPythonStack(Stack):

    def __init__(self, app: App, id: str) -> None:
        super().__init__(app, id)

        #SNS Topic
        MySnsTopic = sns.Topic(
            self, "MySnsTopic"
        )

        #Subscribe an email address to your topic 
        # MySnsTopic.add_subscription(subscriptions.EmailSubscription("shsnket@amazon.com"))

        email_address = CfnParameter(self, "email-param",default='shsnket@amazon.com')
        MySnsTopic.add_subscription(subscriptions.EmailSubscription(email_address.value_as_string))

        # DynamoDB Table
        dynamoTable = _dyn.Table(
            self, "snapshot_manager_status",
            partition_key=_dyn.Attribute(
                name="app_name",
                type=_dyn.AttributeType.STRING
            ),
            sort_key = _dyn.Attribute(
                name="snapshot_manager_run_id",
                type=_dyn.AttributeType.NUMBER
            ),
            table_name = "snapshot_manager_status",
            billing_mode=_dyn.BillingMode.PAY_PER_REQUEST,
        )
        
        # Create the AWS Lambda function to subscribe to Amazon SQS queue
        # The source code is in './lambda' directory
        lambda_function = _lambda.Function(
            self, "MyLambdaFunction",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="kda_flink_snapshot_manager.lambda_handler",
            code=_lambda.Code.from_asset("lambda"),
            environment = {
            'aws_region':	"us-east-1"	,
            'app_name' :	"kda-flink-snapshot_manager",
            'snapshot_manager_ddb_table_name' :	dynamoTable.table_name,
            'primary_partition_key_name' :	dynamoTable.schema().partition_key.name ,#	Primary partition key name
            'primary_sort_key_name' :	dynamoTable.schema().sort_key.name,#	Primary sort key name
            'sns_topic_arn' :	MySnsTopic.topic_arn	,
            'number_of_older_snapshots_to_retain' :	"30",	
            'snapshot_creation_wait_time_seconds' :	"15",
          }
        )

        # Set Lambda Logs Retention and Removal Policy
        logs.LogGroup(
            self,
            'logs',
            log_group_name = f"/aws/lambda/{lambda_function.function_name}",
            removal_policy = RemovalPolicy.DESTROY,
            retention = logs.RetentionDays.ONE_DAY
        )

        #Event Bridge rule
        #Change the rate according to your needs
        rule = events.Rule(self, 'Rule',
           description = "Trigger Lambda function every 15 minutes",
           schedule = events.Schedule.expression('rate(15 minutes)')
        )

        rule.add_target(events_target.LambdaFunction(lambda_function))

        dynamoTable.grant_write_data(lambda_function)
        # Grant publish to lambda function
        MySnsTopic.grant_publish(lambda_function)
       

app = App()
CdkPythonStack(app, "CdkPythonExample",
    # If you don't specify 'env', this stack will be environment-agnostic.
    # Account/Region-dependent features and context lookups will not work,
    # but a single synthesized template can be deployed anywhere.

    # Uncomment the next line to specialize this stack for the AWS Account
    # and Region that are implied by the current CLI configuration.

    #env=core.Environment(account=os.getenv('CDK_DEFAULT_ACCOUNT'), region=os.getenv('CDK_DEFAULT_REGION')),

    # Uncomment the next line if you know exactly what Account and Region you
    # want to deploy the stack to. */

    #env=core.Environment(account='123456789012', region='us-east-1'),

    # For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html
    )

app.synth()


