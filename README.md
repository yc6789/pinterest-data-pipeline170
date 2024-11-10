
# Pinterest Data Pipeline with AWS MSK Kafka

This repository provides the implementation details for creating a Pinterest Data Pipeline using AWS services, specifically leveraging an IAM-authenticated MSK cluster with Kafka on an AWS EC2 instance. The goal is to efficiently manage Pinterest data streams, including posts, geolocation data, and user information.

## Overview

The Pinterest Data Pipeline is designed to:

- Ingest data from various Pinterest sources.
- Use Kafka for data streaming within an AWS MSK cluster.
- Manage and process data for real-time and batch analytics.

This README includes instructions for setting up the required environment, configuring Kafka, and creating necessary Kafka topics to facilitate the data pipeline.

## Prerequisites

- **AWS Account**: Access with permissions to connect to an MSK cluster.
- **EC2 Instance**: An EC2 client machine with security rules configured to communicate with the MSK cluster.
- **IAM Role**: A role for cluster authentication.

## Setup Instructions

### Step 1: Install Kafka

1. Connect to your EC2 instance.
2. Install Kafka version `2.12-2.8.1` on your EC2 instance. Make sure you install the **same version** as the MSK cluster (`2.12-2.8.1`), otherwise communication will fail.
3. The EC2 security group rules have already been set up to allow communication with the MSK cluster.

### Step 2: Install IAM MSK Authentication Package

1. Install the IAM MSK authentication package on your client EC2 machine. This package is necessary to connect to IAM-authenticated MSK clusters.

### Step 3: Configure IAM Role for MSK Authentication

To authenticate to the MSK cluster, modify the trust relationships of an existing IAM role on your AWS account:

1. Navigate to the **IAM Console** on your AWS account.
2. Select **Roles** on the left-hand side.
3. Find and select the role named `<your_UserId>-ec2-access-role`.
4. Copy the role ARN and make a note of it for later use.
5. Go to the **Trust relationships** tab and click **Edit trust policy**.
6. Click **Add a principal** and choose **IAM roles** as the Principal type.
7. Replace the ARN with the ARN you copied for `<your_UserId>-ec2-access-role`.

You can now assume the `<your_UserId>-ec2-access-role`, which contains the necessary permissions to authenticate to the MSK cluster.

### Step 4: Configure Kafka Client for IAM Authentication

1. Open the `client.properties` file located in the `kafka_folder/bin` directory.
2. Modify the `client.properties` file to configure AWS IAM authentication for the MSK cluster.

### Step 5: Retrieve MSK Cluster Information

1. Access the **MSK Management Console** on your AWS account.
2. Retrieve the following information about the MSK cluster:
   - **Bootstrap servers string**
   - **Plaintext Apache Zookeeper connection string**
3. Make a note of these strings, as they will be needed in the next steps.

**Note**: You do not have CLI credentials, so use the **MSK Management Console** to retrieve this information.

### Step 6: Create Kafka Topics

Create the following three topics on the MSK cluster:

- `<your_UserId>.pin` for Pinterest post data
- `<your_UserId>.geo` for post geolocation data
- `<your_UserId>.user` for post user data

#### Steps to Create Topics

1. Ensure your `CLASSPATH` environment variable is set correctly before running Kafka commands.
2. Run the `create topic` command, replacing `BootstrapServerString` with the value retrieved from the MSK console.
3. Use the **exact topic names** listed above to avoid permission errors, as only these specific topic names have been granted permission.

## Project Summary

This repository contains the setup and configuration scripts needed to establish a Pinterest Data Pipeline using AWS MSK Kafka. Follow the instructions carefully to ensure a successful deployment.

If you encounter any issues or need further assistance, please refer to the [AWS Documentation](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html) or contact your project administrator.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Please read our [CONTRIBUTING](CONTRIBUTING.md) guide to get started.
