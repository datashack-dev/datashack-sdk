const { validate } = require('jsonschema');
const { default: athena } = require('./athena');
const resource_config = require('./resource_config');
const { Column, SchemaEditor } = require('./resource_schemas');
const { DatashackSdk } = require('./base');
const AWS = require('aws-sdk');

class Database extends DatashackSdk {
  constructor(database_name, s3_bucket) {
    super(database_name);
    this._resource_config = new resource_config.DatabaseConf(database_name, s3_bucket);
  }
}

class StreamingTable extends DatashackSdk {
  constructor(table_name, database_name, columns = {}, no_shards = 1, kinesis_partition_key = null) {
    super(`kinesis_job_${database_name}_${table_name}`, 'json');
    this._resource_config = new resource_config.StreamingTableConf(table_name, database_name, columns, no_shards);
    this.kinesis_partition_key = kinesis_partition_key;
  }

  get_athena_engine() {
    const rtc = this.get_runtime_context();
    const env_conf = process.env;
    return athena.get_sqlalchemy_engine(
      rtc.database_name,
      env_conf.aws_region,
      `s3://${env_conf.aws_assets_bucket}/athena_results/`,
      env_conf.aws_access_key,
      env_conf.aws_access_secret
    );
  }

  get_table_obj(connection) {
    const { Table, MetaData } = require('sequelize');
    return Table(this._resource_config.table_name, MetaData(), { schema: this._resource_config.database_name });
  }

  static get_kinesis_client() {
    return new AWS.Kinesis();
  }

  insert(data) {
    const rtc = this.get_runtime_context();
    const validated_data = this.validate(data);
    const byte_data = JSON.stringify(validated_data);
    if (this.kinesis_partition_key) {
      return StreamingTable.get_kinesis_client().putRecord({
        StreamName: rtc.kinesis_name,
        Data: byte_data,
        PartitionKey: this.kinesis_partition_key
      }).promise();
    } else {
      return StreamingTable.get_kinesis_client().putRecord({
        StreamName: rtc.kinesis_name,
        Data: byte_data,
        PartitionKey: Object.keys(this._resource_config.columns)[0], // the partition key column is overridden
        ExplicitHashKey: String(parseInt(Math.random() * 10 ** 37)) // random hash key for random shard
      }).promise();
    }
  }

  get_jsonschema() {
    return {
      type: 'object',
      properties: Object.fromEntries(
        Object.entries(this.columns).map(([c, col]) => [c, this.JSON_SCHEMA_MAPPER[col.col_type]])
      ),
      additionalProperties: false,
      required: Object.entries(this.columns).filter(([c, col]) => col.required).map(([c, col]) => c)
    };
  }

  validate(data) {
    validate(data, this.get_jsonschema());
    return data;
  }

  [Symbol.toPrimitive](hint) {
    if (hint === 'number') {
      return Object.values(this.columns).length;
    } else if (hint === 'string') {
      return `StreamingTable_${this._resource_config.database_name}_${this._resource_config.table_name}`;
    } else {
      return `[object StreamingTable(${this._resource_config.database_name}.${this._resource_config.table_name})]`;
    }
  }
}

module.exports = { Database, StreamingTable };
