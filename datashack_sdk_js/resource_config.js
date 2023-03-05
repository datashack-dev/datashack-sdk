const { validate } = require('jsonschema');
const { get_sqlalchemy_engine } = require('./athena');

class DatashackSdk {
  constructor(resourceId) {
    this.resourceId = resourceId;
  }

  get_runtime_context() {
    // implementation goes here
  }
}


class DatabaseConf extends ResourceConfig {
  constructor(databaseName, s3Bucket) {
    super();
    this.databaseName = databaseName;
    this.s3Bucket = s3Bucket;
  }
}

class GlueTableConf extends ResourceConfig {
  constructor(tableName, databaseName, tableExternalLoc, columns) {
    super();
    this.tableName = tableName;
    this.databaseName = databaseName;
    this.tableExternalLoc = tableExternalLoc;
    this.columns = columns;
  }
}

class StreamingTableConf extends ResourceConfig {
  constructor(tableName, databaseName, columns, noShards) {
    super();
    this.tableName = tableName;
    this.databaseName = databaseName;
    this.columns = columns || {};
    this.noShards = noShards || 1;
  }
}

class StreamingTable extends DatashackSdk {
  constructor(tableName, databaseName, columns, noShards, kinesisPartitionKey) {
    super(`kinesis_job_${databaseName}_${tableName}`, 'json');
    this._resourceConfig = new StreamingTableConf(tableName, databaseName, columns, noShards);
    this.kinesisPartitionKey = kinesisPartitionKey;
  }

  get_athena_engine() {
    const rtc = this.get_runtime_context();
    const envConf = {};

    return get_sqlalchemy_engine(
      rtc['database_name'],
      envConf['aws_region'],
      `s3://${envConf['aws_assets_bucket']}/athena_results/`,
      envConf['aws_access_key'],
      envConf['aws_access_secret']
    );
  }

  get_table_obj(connection) {
    // implementation goes here
  }

  static get_kinesis_client() {
    // implementation goes here
  }

  insert(data) {
    const rtc = this.get_runtime_context();
    const validatedData = this.validate(data);
    const byteData = JSON.stringify(validatedData);

    if (this.kinesisPartitionKey) {
      const response = StreamingTable.get_kinesis_client().put_record({
        StreamName: rtc['kinesis_name'],
        Data: byteData,
        PartitionKey: this.kinesisPartitionKey,
      });
      return response;
    }

    const response = StreamingTable.get_kinesis_client().put_record({
      StreamName: rtc['kinesis_name'],
      Data: byteData,
      PartitionKey: Object.keys(this._resourceConfig.columns)[0],
      ExplicitHashKey: String(Math.floor(Math.random() * 10 ** 37)),
    });
    return response;
  }

  get_jsonschema() {
    const schemaMapper = {
      string: 'string',
      int: 'integer',
      double: 'number',
      date: 'string',
      datetime: 'string',
      boolean: 'boolean',
    };

    const required = [];
    const properties = Object.entries(this.columns).reduce((acc, [colName, col]) => {
      if (col.required) {
        required.push(colName);
      }
      acc[colName] = { type: schemaMapper[col.col_type] };
      return acc;
    }, {});

    return {
      type: 'object',
      properties,
      additionalProperties: false,
      required,
    };
  }

  validate(data) {
    validate(data, this.get_jsonschema());
    return data;
  }

 
