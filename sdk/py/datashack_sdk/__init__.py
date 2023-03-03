#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import random
import boto3
from jsonschema.validators import validate
from .base import DatashackSdk
from . import resource_config
from .resource_schemas import SchemaEditor, Column
from . import athena as athena
from typing import Sequence, List, Dict, Optional, Any
from sqlalchemy.sql.schema import Table, MetaData
from .athena import get_sqlalchemy_engine


class Database(DatashackSdk):
    def __init__(self, database_name: str, s3_bucket: str):
        super(Database, self).__init__(resource_id=database_name)

        self._resource_config = resource_config.DatabaseConf(
            database_name=database_name,
            s3_bucket=s3_bucket)


class StreamingTable(DatashackSdk, SchemaEditor):
    def __init__(self,
                 table_name: str,
                 database_name: str,
                 columns: Optional[Dict[str, Column]] = None,
                 no_shards: int = 1,
                 kinesis_partition_key: str = None):
        super().__init__(resource_id=f"kinesis_job_{database_name}_{table_name}", schema_type="json")

        self._resource_config = resource_config.StreamingTableConf(
            table_name,
            database_name,
            columns or {},
            no_shards)

        self.kinesis_partition_key = kinesis_partition_key

    def get_athena_engine(self) -> Sequence:
        rtc = self.get_runtime_context()

        return get_sqlalchemy_engine(database_name=rtc['database_name'],
                                     region=env_conf["aws_region"],
                                     s3_bucket_path=f"s3://{env_conf['aws_assets_bucket']}/athena_results/",
                                     access_key=env_conf['aws_access_key'],
                                     secret_access_key=env_conf['aws_access_secret'])

    def get_table_obj(self, connection):
        return Table(self._resource_config.table_name, MetaData(), autoload_with=connection)

    @staticmethod
    def get_kinesis_client():
        return boto3.client('kinesis')

    def insert(self, data):
        rtc = self.get_runtime_context()
        validated_data = self.validate(data)
        byte_data = json.dumps(validated_data)
        if self.kinesis_partition_key:
            response = self.get_kinesis_client().put_record(
                StreamName=rtc["kinesis_name"],
                Data=byte_data,
                PartitionKey=self.kinesis_partition_key,
                # StreamARN=rtc['kinesis_arn'] not testable trough moto
            )
        else:
            response = self.get_kinesis_client().put_record(
                StreamName=rtc["kinesis_name"],
                Data=byte_data,
                PartitionKey=list(self._resource_config.columns.keys())[0],  # the partition key column is overriden
                ExplicitHashKey=str(int(random.random() * 10 ** 37)),  # random hash key for random shard
                # StreamARN=rtc['kinesis_arn'], not testable trough moto

            )
        return response

    def get_jsonschema(self):
        return {
            "type": "object",
            "properties": {c: self.JSON_SCHEMA_MAPPER[self.columns[c].col_type] for c in self.columns},
            "additionalProperties": False,
            "required": [c for c in self.columns if self.columns[c].required]
        }

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        # JSONSCHEMA is nny

        validate(instance=data, schema=self.get_jsonschema())
        return data

    def __hash__(self):
        """
        hash function for lru cache
        :return:
        """
        return hash(f"StreamingTable_{self._resource_config.database_name}_{self._resource_config.table_name}")
