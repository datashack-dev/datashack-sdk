import json
import os
from cdktf import Fn
from typing import Dict, Union, Type, List
import cdktf_cdktf_provider_aws
from cdktf_cdktf_provider_aws.glue_catalog_database import GlueCatalogDatabase
from cdktf_cdktf_provider_aws.glue_catalog_table import GlueCatalogTable, GlueCatalogTableStorageDescriptor, \
    GlueCatalogTableStorageDescriptorSerDeInfo
from cdktf_cdktf_provider_aws.glue_job import GlueJob, GlueJobCommand, GlueJobExecutionProperty
from cdktf_cdktf_provider_aws.kinesis_stream import KinesisStream, KinesisStreamStreamModeDetails
from cdktf_cdktf_provider_aws.s3_bucket import S3Bucket, S3BucketConfig
from cdktf_cdktf_provider_aws.s3_bucket_object import S3BucketObject
from constructs import Construct
# todo get rid of relative imports
from ..datashack_base import DatashackResourceConf, DatashackPlugin, DatashackResourceOutputs
from collections import defaultdict
from cdktf_cdktf_provider_aws.iam_role import IamRole, IamRoleInlinePolicy
from cdktf_cdktf_provider_aws.data_aws_caller_identity import DataAwsCallerIdentity


class DatabaseConf(DatashackResourceConf):
    """
     is a necessary configuration for the glue plugin, Database will be created and then the tables will
     be set under their database, for example if you want to use StreamingTableConf
     then it will need datanase configured before.
   """

    @classmethod
    def get_plugin_class(cls) -> Type[DatashackPlugin]:
        return GluePlugin

    database_name: str
    location_uri: str
    s3_bucket: str

    def __init__(self, database_name: str, s3_bucket: str):
        """

        :param database_name: database name
        :param s3_bucket: the db will be located under the root of the bucket i.g: s3://<s3-bucket>/<database_name>/
        """
        super().__init__(resource_id=database_name)
        self.config_asserts(database_name)
        self.database_name = database_name
        self.s3_bucket = s3_bucket
        self.location_uri = f"s3://{self.s3_bucket}/{database_name}"

    @classmethod
    def config_asserts(cls, db_name):
        if "_" in db_name:
            raise ValueError(" \'_\' char is ot valid")


class GlueTableConf(DatashackResourceConf):
    @classmethod
    def get_plugin_class(cls) -> Type[DatashackPlugin]:
        return GluePlugin

    table_name: str
    database_name: str
    table_external_loc: str
    columns: List[Dict[str, str]]
    partition_keys: List[Dict[str, str]]

    @staticmethod
    def resource_id_parser(name, database):
        return f"{name}_{database}"

    def __init__(self,
                 table_name: str,
                 database_name: str,
                 columns: List[Dict[str, str]],
                 partition_keys: List[Dict[str, str]],
                 table_external_loc: str, ):
        super().__init__(resource_id=self.resource_id_parser(table_name, database_name))
        self.table_name = table_name
        self.database_name = database_name
        self.partition_keys = partition_keys
        self.columns = columns
        self.table_external_loc = table_external_loc


class StreamingTableConf(DatashackResourceConf):
    # glue table
    table_name: str
    database_name: str
    columns: List[Dict[str, str]]
    partition_keys: List[Dict[str, str]]

    # kinesis streams conf
    stream_name: str
    shard_count: int

    # job conf
    role_arn: str
    stream_job_name: str

    @classmethod
    def get_plugin_class(cls) -> Type[DatashackPlugin]:
        return GluePlugin

    @classmethod
    def config_asserts(cls, db_name):
        if "_" in db_name:
            raise ValueError(" \'_\' char is ot valid")

    def __init__(self, database_name: str, table_name: str, columns: Dict[str, 'Column'], no_shards: int):
        super().__init__(resource_id=f"kinesis_job_{database_name}_{table_name}")
        self.config_asserts(database_name)
        # glue table + db conf
        self.table_name = table_name
        self.database_name = database_name
        self.columns = []
        self.partition_keys = []

        for col_name, col in columns.items():
            col_type = col['col_type']
            col_el = {'name': col_name, 'type': col_type}
            if col.get('partition'):
                self.partition_keys.append(col_el)
            else:
                self.columns.append(col_el)

        self.stream_name = f"{database_name}_{table_name}"  # kinesis queue name
        self.shard_count = no_shards

        self.stream_job_name = f"kinesis_stream_into_{database_name}_{table_name}"


class GluePlugin(DatashackPlugin):

    @staticmethod
    def type_the_resource_id(resource_class, *, resource_id=None, resource_output_id=None):
        parsed_resource_id = f"{resource_class.__name__}_{resource_id}" if resource_id else None
        parsed_resource_output_id = f"{resource_class.__name__}_{resource_output_id}" if resource_output_id else None
        return parsed_resource_id, parsed_resource_output_id

    def __init__(self, scope: Construct, glue_resources: List[DatashackResourceConf], env: str):
        self.scope = scope
        self.env = env
        self.prefix = f'{self.env}-'

        self.resources_outputs = defaultdict(dict)

        self.account_id = DataAwsCallerIdentity(scope, "me").account_id

        dbs_confs = [r for r in glue_resources if isinstance(r, DatabaseConf)]
        self.glue_dbs = self.init_glue_databases(dbs_confs)

        tables_confs = [r for r in glue_resources if
                        isinstance(r, (GlueTableConf, StreamingTableConf))]
        self.glue_tables = self.init_glue_tables(tables_confs)

        kinesis_streams = [r for r in glue_resources if isinstance(r, StreamingTableConf)]
        self.kinesis_streams = self.init_kinesis_streams(kinesis_streams)

        glue_stream_jobs = [r for r in glue_resources if isinstance(r, StreamingTableConf)]
        self.init_glue_stream_jobs(glue_stream_jobs)

        self.create_outputs()

    def init_outputs_obj(self, scope) -> DatashackResourceOutputs:
        pass

    def init_glue_databases(self,
                            dbs_confs: List[Union[DatabaseConf, StreamingTableConf]]) -> Dict:
        glue_dbs = {}
        for db in dbs_confs:
            r_id_typed, r_output_id_typed = self.type_the_resource_id(GlueCatalogDatabase,
                                                                      resource_id=db.resource_id,
                                                                      resource_output_id=db.resource_output_id)
            tf_db_obj = GlueCatalogDatabase(self.scope,
                                            r_id_typed,
                                            name=self.prefix + db.database_name,
                                            location_uri=db.location_uri)

            self.resources_outputs[db]['database_name'] = tf_db_obj.name
            self.resources_outputs[db]['database_location_uri'] = tf_db_obj.location_uri

            glue_dbs[db.resource_id] = tf_db_obj
        return glue_dbs

    def init_glue_tables(self, tables_confs: List[Union[DatabaseConf, StreamingTableConf]]):
        glue_tables = {}
        for table in tables_confs:
            resource_id_typed, resource_output_id_typed = self.type_the_resource_id(GlueCatalogTable,
                                                                                    resource_id=table.resource_id,
                                                                                    resource_output_id=table.resource_output_id)
            tf_table_obj = GlueCatalogTable(self.scope,
                                            resource_id_typed,
                                            name=self.prefix + table.table_name,
                                            database_name=self.prefix + table.database_name,
                                            table_type="EXTERNAL_TABLE",
                                            parameters={
                                                "EXTERNAL": "TRUE",
                                                "parquet.compression": "SNAPPY",
                                                "useGlueParquetWriter": "TRUE"
                                            },
                                            storage_descriptor=GlueCatalogTableStorageDescriptor(
                                                location=f"{self.glue_dbs[table.database_name].location_uri}/{table.table_name}",
                                                input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                                output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                                                ser_de_info=GlueCatalogTableStorageDescriptorSerDeInfo(
                                                    name="my-stream",
                                                    serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                                    parameters={"serialization.format": "1"},
                                                ),
                                                columns=table.columns
                                            ),
                                            partition_keys=table.partition_keys)

            self.resources_outputs[table]['table_name'] = tf_table_obj.name
            self.resources_outputs[table]['database_name'] = tf_table_obj.database_name

            glue_tables[table.resource_id] = tf_table_obj
        return glue_tables

    def init_kinesis_streams(self,
                             kinesis_streams: List[Union[DatabaseConf, StreamingTableConf]]):
        ks_streams = {}
        for ks in kinesis_streams:
            r_id_typed, r_output_id_typed = self.type_the_resource_id(KinesisStream,
                                                                      resource_id=ks.resource_id,
                                                                      resource_output_id=ks.resource_output_id)
            tf_kinesis_obj = KinesisStream(self.scope,
                                           r_id_typed,
                                           name=self.prefix + ks.stream_name,
                                           shard_count=ks.shard_count,
                                           encryption_type="NONE",
                                           retention_period=24,
                                           stream_mode_details=KinesisStreamStreamModeDetails(
                                               stream_mode="PROVISIONED")
                                           )

            self.resources_outputs[ks]['kinesis_arn'] = tf_kinesis_obj.arn
            self.resources_outputs[ks]['kinesis_name'] = tf_kinesis_obj.name

            ks_streams[ks.resource_id] = tf_kinesis_obj
        return ks_streams

    def init_glue_stream_jobs(self,
                              glue_stream_jobs: List[Union[DatabaseConf, StreamingTableConf]]):
        

        tf_glue_assets_bucket = S3Bucket(self.scope, f'{self.prefix}glue-assets')
        assets_bucket = tf_glue_assets_bucket.bucket
        
        script_path = os.path.join(__file__.rsplit("/", 1)[0], "utils/kinessis_stream_template")
        for stream_job_def in glue_stream_jobs:
            script_template_vars = {
                "kinesis_stream_arn": self.kinesis_streams[stream_job_def.resource_id].arn,
                "database_name": self.glue_tables[stream_job_def.resource_id].database_name,
                "table_name": self.glue_tables[stream_job_def.resource_id].name,
                "partition_keys": [k["name"] for k in stream_job_def.partition_keys]
            }

            stream_job_script = Fn.templatefile(script_path, script_template_vars)
            r_id_typed, r_output_id_typed = self.type_the_resource_id(S3BucketObject,
                                                                      resource_id=stream_job_def.resource_id,
                                                                      resource_output_id=stream_job_def.resource_output_id)
            tf_python_obj = S3BucketObject(self.scope,
                                           r_id_typed,
                                           bucket=assets_bucket,
                                           key=f"kinessis_stream_{stream_job_def.database_name}_{stream_job_def.table_name}.py",
                                           content=stream_job_script,
                                           etag=stream_job_script)

            tf_glue_role = IamRole(self.scope, f'{self.prefix}{stream_job_def.stream_job_name}-role',
                      name=f'{stream_job_def.stream_job_name}-role',
                      description=f'Role for Glue stream {stream_job_def.stream_job_name}',
                      assume_role_policy=json.dumps({
                                    'Version': '2012-10-17',
                                    'Statement': [
                                        {
                                            'Action': 'sts:AssumeRole',
                                            'Sid': '',
                                            'Effect': 'Allow',
                                            'Principal': {
                                                'Service': "glue.amazonaws.com"
                                            }
                                        }
                                    ]
                                }),
                      inline_policy=[
                            IamRoleInlinePolicy(
                                name='inline-policy-1',
                                policy=json.dumps({
                                    'Version': '2012-10-17',
                                    'Statement': [
                                        {
                                            'Action': ['s3:*', 'glue:*', 'kinesis:*', 'logs:*', 'cloudwatch:*'],
                                            'Effect': 'Allow',
                                            'Resource': ['*']
                                        }
                                    ]
                                })
                            )
                        ]
            )

            r_id_typed, r_output_id_typed = self.type_the_resource_id(GlueJob,
                                                                      resource_id=stream_job_def.resource_id,
                                                                      resource_output_id=stream_job_def.resource_output_id)
            tf_glue_job = GlueJob(self.scope,
                                  r_id_typed,
                                  execution_class="STANDARD",
                                  glue_version="3.0",
                                  max_retries=0,
                                  number_of_workers=2,
                                  worker_type="G.025X",
                                  name=self.prefix + stream_job_def.stream_job_name,
                                  command=GlueJobCommand(name="gluestreaming", python_version="3",
                                                         script_location=f"s3://{tf_python_obj.bucket}/{tf_python_obj.key}"),
                                  role_arn=tf_glue_role.arn,
                                  default_arguments={
                                      "--TempDir": f"s3://{assets_bucket}/temporary/",
                                      "--enable-continuous-cloudwatch-log": "true",
                                      "--enable-glue-datacatalog": "true",
                                      "--enable-job-insights": "false",
                                      "--enable-metrics": "true",
                                      "--enable-spark-ui": "true",
                                      "--job-bookmark-option": "job-bookmark-disable",
                                      "--job-language": "python",
                                      "--spark-event-logs-path": f"s3://{assets_bucket}/sparkHistoryLogs/"
                                  },
                                  execution_property=GlueJobExecutionProperty(max_concurrent_runs=1)

                                  )

    def create_outputs(self):

        for resource, output in self.resources_outputs.items():
            output_id = resource.resource_output_id
            DatashackResourceOutputs(self.scope, output_id, output)
