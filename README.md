# Datashack

## What is this?

> Create AWS Kinesis+Glue pipeline with Datashack SDK.

For example:

```python
from datashack_sdk_py import StreamingTable, Column

db = Database("db1", "tables-bucket")

UserEvents = StreamingTable("users", "db1")

UserEvents['id'] = Column('string')
UserEvents['age'] = Column('int')
UserEvents['name'] = Column('string')
UserEvents['ts'] = Column('timestamp')

```

run `datashack plan/apply` to see the actual changes or actually applying them to your AWS account.  

creates this pipeline:

![Tux, the Linux mascot](https://raw.githubusercontent.com/datashack-dev/datashack-sdk/main/md/aws1.png)

- Provision Kinesis+Spark+Glue+Iam
- Automate schema evolution
- Tests


## Pre-requisites

To work with this project, you will need to have the following software installed on your machine:

- **Node.js**: You can download and install Node.js from the official website: [https://nodejs.org/en/download/](https://nodejs.org/en/download/)
- **Python**: You can download and install Python from the official website: [https://www.python.org/downloads/](https://www.python.org/downloads/)
- **Terraform**: These tools are used for defining infrastructure as code. To install them, follow the instructions on the HashiCorp website: [https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli). 
- **CDKTF**: you can install the CDKTF CLI by running the following command: 

    ```
    npm install --global cdktf-cli@latest
    ```
- **AWS Account**: create an AWS account if you don't have one and configure your local computer aws configuration with `aws config` - https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html

## Getting Started

run in your terminal 

```
pip install datashack-sdk
git clone https://github.com/datashack-dev/datashack-sdk-examples
datashack plan ./datashack-sdk-examples/my_app/models
```


## Roadmap

- Additional AWS services
- Additional data sources and sinks, such as Apache Kafka or Elasticsearch
- 3rd Party sources - Google Analytics, Salesforce ...
- Spark based transformation and processing capabilities
- Integration with CICD workflows
- Integration with runtime applications
- Testing
- Automatic documentation generation
- More languages support for Datashack SDK: Go, Java, Yaml


## Stay tuned

We are working on a fully funcional beta with many more features. Join here so we can ping you 

https://www.datashack.dev/stay-in-touch


