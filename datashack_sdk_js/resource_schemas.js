class Column {
  constructor(col_type, partition = false, required = true) {
    if (!SchemaEditor.JSON_SCHEMA_MAPPER[col_type]) {
      throw `${col_type} is not a supported type`;
    }
    this.col_type = col_type;
    this.partition = partition;
    this.required = required;
  }

  __add__(x) {
    // tbd
  }

  __sub__(x) {
    // tbd
  }

  __mul__(x) {
    // tbd
  }

  __truediv__(x) {
    // tbd
  }

  __eq__(x) {
    return ConditionEq(this, x);
  }

  __str__() {
    return this.table + '.' + this.col_name;
  }

  is_assigned_to_table() {
    return Boolean(this._table);
  }

  set_table(table) {
    this._table = table;
  }

  get table() {
    return this._table;
  }
}

class SchemaEditor {
  static JSON_SCHEMA_MAPPER = {
    "string": { "type": "string" },
    "str": { "type": "string" },
    "int": { "type": "number" },
    "double": { "type": "number" },
    "integer": { "type": "number" },
    "map": { "type": "object" },
    "array": { "type": "array" },
    "date": { "type": "string", "format": "date" },
    "timestamp": { "type": "string", "format": "date-time" },
    "boolean": { "type": "booelan" },
  };

  constructor(schema_type = "json", kwargs) {
    super(kwargs);
    this.schema_type = schema_type.toLowerCase();
  }

  get(key) {
    return this._resource_config.columns[key];
  }

  set(key, value) {
    this.validate_type(value.col_type);
    this._resource_config.columns[key] = value;
  }

  delete(key) {
    delete this._resource_config.columns[key];
  }

  *[Symbol.iterator]() {
    yield* Object.entries(this._resource_config.columns);
  }

  get length() {
    return Object.keys(this._resource_config.columns).length;
  }

  get columns() {
    return Object.fromEntries(this.entries());
  }

  get schema() {
    return;
  }

  validate_type(type_name) {
    if (!["string", "int", "integer", "bigint", "double", "boolean", "date", "timestamp", "array", "map", "struct"].includes(type_name)) {
      throw "column_type is not jsonschema type";
    }
  }
}
