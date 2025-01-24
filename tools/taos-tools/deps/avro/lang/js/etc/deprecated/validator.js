// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var _ = require("underscore"),
    util = require('util');

var WARNING = 'Validator API is deprecated. Please use the type API instead.';
Validator = util.deprecate(Validator, WARNING);
ProtocolValidator = util.deprecate(ProtocolValidator, WARNING);

var AvroSpec = {
  PrimitiveTypes: ['null', 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string'],
  ComplexTypes: ['record', 'enum', 'array', 'map', 'union', 'fixed']
};
AvroSpec.Types = AvroSpec.PrimitiveTypes.concat(AvroSpec.ComplexTypes);

var InvalidSchemaError = function(msg) { return new Error('InvalidSchemaError: ' + msg); };
var InvalidProtocolError = function(msg) { return new Error('InvalidProtocolError: ' + msg); };
var ValidationError = function(msg) { return new Error('ValidationError: ' + msg); };
var ProtocolValidationError = function(msg) { return new Error('ProtocolValidationError: ' + msg); };


function Record(name, namespace, fields) {
  function validateArgs(name, namespace, fields) {
    if (!_.isString(name)) {
      throw new InvalidSchemaError('Record name must be string');
    }

    if (!_.isNull(namespace) && !_.isUndefined(namespace) && !_.isString(namespace)) {
      throw new InvalidSchemaError('Record namespace must be string or null');
    }

    if (!_.isArray(fields)) {
      throw new InvalidSchemaError('Record name must be string');
    }
  }

  validateArgs(name, namespace, fields);

  this.name = name;
  this.namespace = namespace;
  this.fields = fields;
}

function makeFullyQualifiedTypeName(schema, namespace) {
  var typeName = null;
  if (_.isString(schema)) {
    typeName = schema;
  } else if (_.isObject(schema)) {
    if (_.isString(schema.namespace)) {
      namespace = schema.namespace;
    }
    if (_.isString(schema.name)) {
      typeName = schema.name;
    } else if (_.isString(schema.type)) {
      typeName = schema.type;
    }
  } else {
    throw new InvalidSchemaError('unable to determine fully qualified type name from schema ' + JSON.stringify(schema) + ' in namespace ' + namespace);
  }

  if (!_.isString(typeName)) {
    throw new InvalidSchemaError('unable to determine type name from schema ' + JSON.stringify(schema) + ' in namespace ' + namespace);
  }

  if (typeName.indexOf('.') !== -1) {
    return typeName;
  } else if (_.contains(AvroSpec.PrimitiveTypes, typeName)) {
    return typeName;
  } else if (_.isString(namespace)) {
    return namespace + '.' + typeName;
  } else {
    return typeName;
  }
}

function Union(typeSchemas, namespace) {
  this.branchNames = function() {
    return _.map(typeSchemas, function(typeSchema) { return makeFullyQualifiedTypeName(typeSchema, namespace); });
  };

  function validateArgs(typeSchemas) {
    if (!_.isArray(typeSchemas) || _.isEmpty(typeSchemas)) {
      throw new InvalidSchemaError('Union must have at least 1 branch');
    }
  }

  validateArgs(typeSchemas);

  this.typeSchemas = typeSchemas;
  this.namespace = namespace;
}

function Enum(symbols) {

  function validateArgs(symbols) {
    if (!_.isArray(symbols)) {
      throw new InvalidSchemaError('Enum must have array of symbols, got ' + JSON.stringify(symbols));
    }
    if (!_.all(symbols, function(symbol) { return _.isString(symbol); })) {
      throw new InvalidSchemaError('Enum symbols must be strings, got ' + JSON.stringify(symbols));
    }
  }

  validateArgs(symbols);

  this.symbols = symbols;
}

function AvroArray(itemSchema) {

  function validateArgs(itemSchema) {
    if (_.isNull(itemSchema) || _.isUndefined(itemSchema)) {
      throw new InvalidSchemaError('Array "items" schema should not be null or undefined');
    }
  }

  validateArgs(itemSchema);

  this.itemSchema = itemSchema;
}

function Map(valueSchema) {

  function validateArgs(valueSchema) {
    if (_.isNull(valueSchema) || _.isUndefined(valueSchema)) {
      throw new InvalidSchemaError('Map "values" schema should not be null or undefined');
    }
  }

  validateArgs(valueSchema);

  this.valueSchema = valueSchema;
}

function Field(name, schema) {
  function validateArgs(name, schema) {
    if (!_.isString(name)) {
      throw new InvalidSchemaError('Field name must be string');
    }
  }

  this.name = name;
  this.schema = schema;
}

function Primitive(type) {
  function validateArgs(type) {
    if (!_.isString(type)) {
      throw new InvalidSchemaError('Primitive type name must be a string');
    }

    if (!_.contains(AvroSpec.PrimitiveTypes, type)) {
      throw new InvalidSchemaError('Primitive type must be one of: ' + JSON.stringify(AvroSpec.PrimitiveTypes) + '; got ' + type);
    }
  }

  validateArgs(type);

  this.type = type;
}

function Validator(schema, namespace, namedTypes) {
  this.validate = function(obj) {
    return _validate(this.schema, obj);
  };

  var _validate = function(schema, obj) {
    if (schema instanceof Record) {
      return _validateRecord(schema, obj);
    } else if (schema instanceof Union) {
      return _validateUnion(schema, obj);
    } else if (schema instanceof Enum) {
      return _validateEnum(schema, obj);
    } else if (schema instanceof AvroArray) {
      return _validateArray(schema, obj);
    } else if (schema instanceof Map) {
      return _validateMap(schema, obj);
    } else if (schema instanceof Primitive) {
      return _validatePrimitive(schema, obj);
    } else {
      throw new InvalidSchemaError('validation not yet implemented: ' + JSON.stringify(schema));
    }
  };

  var _validateRecord = function(schema, obj) {
    if (!_.isObject(obj) || _.isArray(obj)) {
      throw new ValidationError('Expected record Javascript type to be non-array object, got ' + JSON.stringify(obj));
    }

    var schemaFieldNames = _.pluck(schema.fields, 'name').sort();
    var objFieldNames = _.keys(obj).sort();
    if (!_.isEqual(schemaFieldNames, objFieldNames)) {
      throw new ValidationError('Expected record fields ' + JSON.stringify(schemaFieldNames) + '; got ' + JSON.stringify(objFieldNames));
    }

    return _.all(schema.fields, function(field) {
      return _validate(field.schema, obj[field.name]);
    });
  };

  var _validateUnion = function(schema, obj) {
    if (_.isObject(obj)) {
      if (_.isArray(obj)) {
        throw new ValidationError('Expected union Javascript type to be non-array object (or null), got ' + JSON.stringify(obj));
      } else if (_.size(obj) !== 1) {
        throw new ValidationError('Expected union Javascript object to be object with exactly 1 key (or null), got ' + JSON.stringify(obj));
      } else {
        var unionBranch = _.keys(obj)[0];
        if (unionBranch === "") {
          throw new ValidationError('Expected union Javascript object to contain non-empty string branch, got ' + JSON.stringify(obj));
        }
        if (_.contains(schema.branchNames(), unionBranch)) {
          return true;
        } else {
          throw new ValidationError('Expected union branch to be one of ' + JSON.stringify(schema.branchNames()) + '; got ' + JSON.stringify(unionBranch));
        }
      }
    } else if (_.isNull(obj)) {
      if (_.contains(schema.branchNames(), 'null')) {
        return true;
      } else {
        throw new ValidationError('Expected union branch to be one of ' + JSON.stringify(schema.branchNames()) + '; got ' + JSON.stringify(obj));
      }
    } else {
      throw new ValidationError('Expected union Javascript object to be non-array object of size 1 or null, got ' + JSON.stringify(obj));
    }
  };

  var _validateEnum = function(schema, obj) {
    if (_.isString(obj)) {
      if (_.contains(schema.symbols, obj)) {
        return true;
      } else {
        throw new ValidationError('Expected enum value to be one of ' + JSON.stringify(schema.symbols) + '; got ' + JSON.stringify(obj));
      }
    } else {
      throw new ValidationError('Expected enum Javascript object to be string, got ' + JSON.stringify(obj));
    }
  };

  var _validateArray = function(schema, obj) {
    if (_.isArray(obj)) {
      return _.all(obj, function(member) { return _validate(schema.itemSchema, member); });
    } else {
      throw new ValidationError('Expected array Javascript object to be array, got ' + JSON.stringify(obj));
    }
  };

  var _validateMap = function(schema, obj) {
    if (_.isObject(obj) && !_.isArray(obj)) {
      return _.all(obj, function(value) { return _validate(schema.valueSchema, value); });
    } else if (_.isArray(obj)) {
      throw new ValidationError('Expected map Javascript object to be non-array object, got array ' + JSON.stringify(obj));
    } else {
      throw new ValidationError('Expected map Javascript object to be non-array object, got ' + JSON.stringify(obj));
    }
  };

  var _validatePrimitive = function(schema, obj) {
    switch (schema.type) {
      case 'null':
        if (_.isNull(obj) || _.isUndefined(obj)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript null or undefined for Avro null, got ' + JSON.stringify(obj));
        }
        break;
      case 'boolean':
        if (_.isBoolean(obj)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript boolean for Avro boolean, got ' + JSON.stringify(obj));
        }
        break;
      case 'int':
        if (_.isNumber(obj) && Math.floor(obj) === obj && Math.abs(obj) <= Math.pow(2, 31)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript int32 number for Avro int, got ' + JSON.stringify(obj));
        }
        break;
      case 'long':
        if (_.isNumber(obj) && Math.floor(obj) === obj && Math.abs(obj) <= Math.pow(2, 63)) {
          return true;
        } else {
          throw new ValidationError('Expected Javascript int64 number for Avro long, got ' + JSON.stringify(obj));
        }
        break;
      case 'float':
        if (_.isNumber(obj)) { // TODO: handle NaN?
          return true;
        } else {
          throw new ValidationError('Expected Javascript float number for Avro float, got ' + JSON.stringify(obj));
        }
        break;
      case 'double':
        if (_.isNumber(obj)) { // TODO: handle NaN?
          return true;
        } else {
          throw new ValidationError('Expected Javascript double number for Avro double, got ' + JSON.stringify(obj));
        }
        break;
      case 'bytes':
        throw new InvalidSchemaError('not yet implemented: ' + schema.type);
      case 'string':
        if (_.isString(obj)) { // TODO: handle NaN?
          return true;
        } else {
          throw new ValidationError('Expected Javascript string for Avro string, got ' + JSON.stringify(obj));
        }
        break;
      default:
        throw new InvalidSchemaError('unrecognized primitive type: ' + schema.type);
    }
  };

  // TODO: namespace handling is rudimentary. multiple namespaces within a certain nested schema definition
  // are probably buggy.
  var _namedTypes = namedTypes || {};
  var _saveNamedType = function(fullyQualifiedTypeName, schema) {
    if (_.has(_namedTypes, fullyQualifiedTypeName)) {
      if (!_.isEqual(_namedTypes[fullyQualifiedTypeName], schema)) {
        throw new InvalidSchemaError('conflicting definitions for type ' + fullyQualifiedTypeName + ': ' + JSON.stringify(_namedTypes[fullyQualifiedTypeName]) + ' and ' + JSON.stringify(schema));
      }
    } else {
      _namedTypes[fullyQualifiedTypeName] = schema;
    }
  };

  var _lookupTypeByFullyQualifiedName = function(fullyQualifiedTypeName) {
    if (_.has(_namedTypes, fullyQualifiedTypeName)) {
      return _namedTypes[fullyQualifiedTypeName];
    } else {
      return null;
    }
  };

  var _parseNamedType = function(schema, namespace) {
    if (_.contains(AvroSpec.PrimitiveTypes, schema)) {
      return new Primitive(schema);
    } else if (!_.isNull(_lookupTypeByFullyQualifiedName(makeFullyQualifiedTypeName(schema, namespace)))) {
      return _lookupTypeByFullyQualifiedName(makeFullyQualifiedTypeName(schema, namespace));
    } else {
      throw new InvalidSchemaError('unknown type name: ' + JSON.stringify(schema) + '; known type names are ' + JSON.stringify(_.keys(_namedTypes)));
    }
  };

  var _parseSchema = function(schema, parentSchema, namespace) {
    if (_.isNull(schema) || _.isUndefined(schema)) {
      throw new InvalidSchemaError('schema is null, in parentSchema: ' + JSON.stringify(parentSchema));
    } else if (_.isString(schema)) {
      return _parseNamedType(schema, namespace);
    } else if (_.isObject(schema) && !_.isArray(schema)) {
      if (schema.type === 'record') {
        var newRecord = new Record(schema.name, schema.namespace, _.map(schema.fields, function(field) {
          return new Field(field.name, _parseSchema(field.type, schema, schema.namespace || namespace));
        }));
        _saveNamedType(makeFullyQualifiedTypeName(schema, namespace), newRecord);
        return newRecord;
      } else if (schema.type === 'enum') {
        if (_.has(schema, 'symbols')) {
          var newEnum = new Enum(schema.symbols);
          _saveNamedType(makeFullyQualifiedTypeName(schema, namespace), newEnum);
          return newEnum;
        } else {
          throw new InvalidSchemaError('enum must specify symbols, got ' + JSON.stringify(schema));
        }
      } else if (schema.type === 'array') {
        if (_.has(schema, 'items')) {
          return new AvroArray(_parseSchema(schema.items, schema, namespace));
        } else {
          throw new InvalidSchemaError('array must specify "items" schema, got ' + JSON.stringify(schema));
        }
      } else if (schema.type === 'map') {
        if (_.has(schema, 'values')) {
          return new Map(_parseSchema(schema.values, schema, namespace));
        } else {
          throw new InvalidSchemaError('map must specify "values" schema, got ' + JSON.stringify(schema));
        }
      } else if (_.has(schema, 'type') && _.contains(AvroSpec.PrimitiveTypes, schema.type)) {
        return _parseNamedType(schema.type, namespace);
      } else {
        throw new InvalidSchemaError('not yet implemented: ' + schema.type);
      }
    } else if (_.isArray(schema)) {
      if (_.isEmpty(schema)) {
        throw new InvalidSchemaError('unions must have at least 1 branch');
      }
      var branchTypes = _.map(schema, function(branchType) { return _parseSchema(branchType, schema, namespace); });
      return new Union(branchTypes, namespace);
    } else {
      throw new InvalidSchemaError('unexpected Javascript type for schema: ' + (typeof schema));
    }
  };

  this.rawSchema = schema;
  this.schema = _parseSchema(schema, null, namespace);
}

Validator.validate = function(schema, obj) {
  return (new Validator(schema)).validate(obj);
}

function ProtocolValidator(protocol) {
  this.validate = function(typeName, obj) {
    var fullyQualifiedTypeName = makeFullyQualifiedTypeName(typeName, protocol.namespace);
    if (!_.has(_typeSchemaValidators, fullyQualifiedTypeName)) {
      throw new ProtocolValidationError('Protocol does not contain definition for type ' + JSON.stringify(fullyQualifiedTypeName) + ' (fully qualified from input "' + typeName + '"); known types are ' + JSON.stringify(_.keys(_typeSchemaValidators)));
    }
    return _typeSchemaValidators[fullyQualifiedTypeName].validate(obj);
  };

  var _typeSchemaValidators = {};
  var _initSchemaValidators = function(protocol) {
    var namedTypes = {};
    if (!_.has(protocol, 'protocol') || !_.isString(protocol.protocol)) {
      throw new InvalidProtocolError('Protocol must contain a "protocol" attribute with a string value');
    }
    if (_.isArray(protocol.types)) {
      _.each(protocol.types, function(typeSchema) {
        var schemaValidator = new Validator(typeSchema, protocol.namespace, namedTypes);
        var fullyQualifiedTypeName = makeFullyQualifiedTypeName(typeSchema, protocol.namespace);
        _typeSchemaValidators[fullyQualifiedTypeName] = schemaValidator;
      });
    }
  };

  _initSchemaValidators(protocol);
}

ProtocolValidator.validate = function(protocol, typeName, obj) {
  return (new ProtocolValidator(protocol)).validate(typeName, obj);
};

if (typeof exports !== 'undefined') {
  exports['Validator'] = Validator;
  exports['ProtocolValidator'] = ProtocolValidator;
}
