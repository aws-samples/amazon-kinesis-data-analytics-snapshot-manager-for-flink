
# CDK Python project!

Snapshot Manager - [Amazon Kinesis Data Analytics](https://docs.aws.amazon.com/kinesisanalytics/latest/java/how-it-works.html) for Apache Flink offers the following benefits:

   1. takes a new snapshot of a running Kinesis Data Analytics for Apache Flink  Application
   1. gets a count of application snapshots
   1. checks if the count is more than the required number of snapshots
   1. deletes older snapshots that are older than the required number

This will be deployed as an [AWS Lambda](https://aws.amazon.com/lambda/) function and scheduled using [Amazon EventBridge rules](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-rules.html) e.g. once in a day or week.


---
## Requirements

* [Create an AWS account](https://portal.aws.amazon.com/gp/aws/developer/registration/index.html) if you do not already have one and log in. The IAM user that you use must have sufficient permissions to make necessary AWS service calls and manage AWS resources.
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) installed and configured
* [Git Installed](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [AWS Cloud Development Kit](https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html) (AWS CDK >= 1.124.0) Installed
* Python 3.7
* IDE e.g. [PyCharm](https://www.jetbrains.com/pycharm/)
* A running Kinesis Data Analytics for Apache Flink Application

---
## Deployment Instructions


The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.
> Provide following required lambda function environment variables through the --context option to the cdk command. Pass runtime context values to your CDK app during synthesis or deployment.
> Required context values for this app are `app_name`, `snapshots_to_retain` and `snapshot_wait_time_seconds`.

```
$ cdk synth \
--context app_name=my-kda-app \
--context snapshots_to_retain=30 \
--context snapshot_wait_time_seconds=15 
```
From the command line, use CDK to deploy the stack:
   
```bash
cdk deploy \
--context app_name=my-kda-app \
--context snapshots_to_retain=30 \
--context snapshot_wait_time_seconds=15 
```

Note the outputs from the CDK deployment process. These contain the resource names and/or ARNs which are used for testing.

Subscribe your email address to the SNS topic:
```bash
aws sns subscribe --topic-arn ENTER_YOUR_TOPIC_ARN --protocol email-json --notification-endpoint ENTER_YOUR_EMAIL_ADDRESS
```

Click the confirmation link(SubscribeURL) delivered to your email to verify the endpoint.

---
## How it works

The CDK stack deploys the resources and the IAM permissions required to run the application.
This stack will deploy Dynamodb, SNS Topic, lambda function and EventBridge Rule.

<TODO: Application details>
 
## Steps for Testing

* <TODO: Steps for testing>
---
## Cleanup
 
1. Run the given command to delete the resources that were created. It might take some time for the CloudFormation stack to get deleted.

    :warning: This will delete Dynamodb table defind as part of this CDK stack(Table name is provided in the CDK stack outputs)!

    ```bash
    cdk destroy \
    --context app_name=my-kda-app \
    --context snapshots_to_retain=30 \
    --context snapshot_wait_time_seconds=15
    ```
1. Deactivate the virtual environment:
    ```bash
    deactivate
    ```
----
Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.

SPDX-License-Identifier: MIT-0

