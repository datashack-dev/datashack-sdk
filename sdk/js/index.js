const DatashackSdk = require('datashack_sdk_js');
const configPb = require('./config_pb.js');

class GlueTable extends DatashackSdk{
    constructor(resourceId, database=null, tableName=null){
        super();
        this.config = new configPb.GlueTableConf();
        this.config.setResourceId(resourceId);
        this.config.setDatabase(database);
        this.config.setName(tableName);
    }

    async query(){
        const rtc = await this.getRuntimeContext();
        const db = rtc.data['database']
        const table = rtc.data['table_name']
        const sql = `select * from ${db}.${table}`;

    }
}

table = new GlueTable('table1');
console.log(table.query());
