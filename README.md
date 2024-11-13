
# Pinterest Data Pipeline with AWS MSK Kafka

This repository provides an overview of the implementation details for creating a Pinterest Data Pipeline using AWS services. The pipeline leverages an IAM-authenticated MSK cluster with Kafka on an AWS EC2 instance, as well as Amazon S3 for data storage. The goal is to efficiently manage Pinterest data streams, including posts, geolocation data, and user information.

## Overview

The Pinterest Data Pipeline is designed to:

- Ingest data from various Pinterest sources.
- Use Kafka for data streaming within an AWS MSK cluster.
- Manage and process data for real-time and batch analytics.
- Store data in an Amazon S3 bucket.

This repository includes instructions for setting up the required environment, configuring Kafka, creating necessary Kafka topics, and using MSK Connect to stream data to an S3 bucket.

## Prerequisites

- **AWS Account**: Access with permissions to connect to an MSK cluster.
- **EC2 Instance**: An EC2 client machine with security rules configured to communicate with the MSK cluster.
- **IAM Role**: A role for cluster authentication.

## Project Setup

### Kafka Installation

1. Connect to your EC2 instance.
2. Install Kafka version `2.12-2.8.1` on your EC2 instance. Ensure you install the **same version** as the MSK cluster (`2.12-2.8.1`) to ensure compatibility.
3. The EC2 security group rules have been pre-configured to allow communication with the MSK cluster.

### IAM MSK Authentication Package Installation

Install the IAM MSK authentication package on your EC2 machine. This package is necessary for connecting to IAM-authenticated MSK clusters.

### IAM Role Configuration for MSK Authentication

To authenticate to the MSK cluster, you must modify the trust relationships of an existing IAM role on your AWS account:

1. Navigate to the **IAM Console** on your AWS account.
2. Select **Roles** on the left-hand side.
3. Find and select the role named `<your_UserId>-ec2-access-role`.
4. Copy the role ARN and make a note of it.
5. Go to the **Trust relationships** tab and click **Edit trust policy**.
6. Click **Add a principal** and choose **IAM roles** as the Principal type.
7. Replace the ARN with the ARN you copied for `<your_UserId>-ec2-access-role`.

You can now assume the `<your_UserId>-ec2-access-role`, which contains the necessary permissions to authenticate to the MSK cluster.

### Kafka Client Configuration for IAM Authentication

1. Open the `client.properties` file located in the `kafka_folder/bin` directory.
2. Modify the `client.properties` file to configure AWS IAM authentication for the MSK cluster.

### Retrieve MSK Cluster Information

1. Access the **MSK Management Console** on your AWS account.
2. Retrieve the following information about the MSK cluster:
   - **Bootstrap servers string**
   - **Plaintext Apache Zookeeper connection string**
3. Make a note of these strings, as they will be needed in the next steps.

**Note**: You do not have CLI credentials, so use the **MSK Management Console** to retrieve this information.

### Create Kafka Topics

Create the following three topics on the MSK cluster:

- `<your_UserId>.pin` for Pinterest post data
- `<your_UserId>.geo` for post geolocation data
- `<your_UserId>.user` for post user data

Ensure your `CLASSPATH` environment variable is set correctly before running Kafka commands. Use the **exact topic names** listed above to avoid permission errors, as only these specific topic names have been granted permission.

## Amazon S3 and MSK Connect Configuration

For this project, the S3 bucket, IAM role, and VPC Endpoint to S3 have been pre-configured in your AWS account.

### Locate S3 Bucket

1. Go to the **S3 Console** and find the bucket that contains your UserId. The bucket name should have the following format: `user-<your_UserId>-bucket`.
2. Make a note of the bucket name.

### Confluent.io Amazon S3 Connector Setup

1. On your EC2 client, download the **Confluent.io Amazon S3 Connector**.
2. Copy the connector to the S3 bucket identified in the previous step.

### Create Custom Plugin in MSK Connect

1. Create your custom plugin in the **MSK Connect Console**.
2. Use the following name for your plugin: `<your_UserId>-plugin`.
3. Create a connector with the name: `<your_UserId>-connector`.
4. Configure the connector with the following settings:
   - Set the **bucket name** to `user-<your_UserId>-bucket`.
   - Set the `topics.regex` field to `<your_UserId>.*` to ensure all the previously created Kafka topics are saved to the S3 bucket.
   - In the **Access permissions** tab, choose the IAM role used for MSK authentication (`<your_UserId>-ec2-access-role`).

With the plugin-connector pair configured, data passing through the IAM-authenticated cluster will be automatically stored in the designated S3 bucket.

## Project Summary

This repository contains the configuration and scripts necessary to set up a Pinterest Data Pipeline using AWS MSK Kafka and Amazon S3. Follow the instructions carefully to ensure a successful deployment.

If you encounter any issues, please refer to the [AWS Documentation](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html) or contact your project administrator.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Please read our [CONTRIBUTING](CONTRIBUTING.md) guide to get started.
