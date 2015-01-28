'use strict';

var Joi      = require('joi'),
    nodeUUID = require('node-uuid'),
    _        = require('lodash');

var internals =  {};

internals.secondaryIndexSchema = Joi.object().keys({
  hashKey : Joi.string().when('type', { is: 'local', then: Joi.ref('$hashKey'), otherwise : Joi.required()}),
  rangeKey: Joi.string().when('type', { is: 'local', then: Joi.required(), otherwise: Joi.optional() }),
  type : Joi.string().valid('local', 'global').required(),
  name : Joi.string().required(),
  projection : Joi.object(),
  readCapacity : Joi.number().when('type', { is: 'global', then: Joi.optional(), otherwise : Joi.forbidden()}),
  writeCapacity : Joi.number().when('type', { is: 'global', then: Joi.optional(), otherwise : Joi.forbidden()})
});

internals.configSchema = Joi.object().keys({
  hashKey   : Joi.string().required(),
  rangeKey  : Joi.string(),
  tableName : Joi.alternatives().try(Joi.string(), Joi.func()),
  indexes   : Joi.array().includes(internals.secondaryIndexSchema),
  schema    : Joi.object(),
  timestamps : Joi.boolean().default(false),
  useAlias : Joi.boolean().default(false)
}).required();

internals.wireType = function (key) {
  switch (key) {
    case 'string':
      return 'S';
    case 'date':
      return 'DATE';
    case 'number':
      return 'N';
    case 'boolean':
      return 'BOOL';
    case 'binary':
      return 'B';
    case 'array':
      return 'L';
    default:
      return null;
  }
};

internals.findDynamoTypeMetadata = function (data) {
  var meta = _.find(data.meta, function (data) {
    return _.isString(data.dynamoType);
  });

  if(meta) {
    return meta.dynamoType;
  } else {
    return internals.wireType(data.type);
  }
};

internals.parseDynamoTypes = function (data) {
  if(_.isPlainObject(data) && data.type === 'object' && _.isPlainObject(data.children)) {
    return internals.parseDynamoTypes(data.children);
  }

  var mapped = _.reduce(data, function(result, val, key) {
    if(val.type === 'object' && _.isPlainObject(val.children)) {
      result[key] = internals.parseDynamoTypes(val.children);
    } else {
      result[key] = internals.findDynamoTypeMetadata(val);
    }

    return result;
  }, {});

  return mapped;
};

var Schema = module.exports = function (config) {
  var self = this;
  
  this.secondaryIndexes = {};
  this.globalIndexes = {};

  var context = {};
  
  if(config.useAlias){
    this._mapAlias = this.createMapAlias(config.schema);
    this._mapName = this.createMapName(config.schema);
    config.schema = this.convertToName(config.schema);

    config.hashKey = this._mapName[config.hashKey].value;
    if(config.rangeKey)
    {
      config.rangeKey = this._mapName[config.rangeKey].value;
    }
    if(config.indexes) {
      _.each(config.indexes,function(c) {
        var obj = {};
         _.each(c,function(c, k) {
           if(['hashKey','rangeKey'].indexOf(k) !== -1){
             obj[k] = self._mapName[c].value;
           }
         });
         c = _.merge(c,obj);
      });
    }
  }
  context = {hashKey : config.hashKey};

  Joi.validate(config, internals.configSchema, { context: context }, function (err, data) {
    if(err) {
      var msg = 'Invalid table schema, check your config ';
      throw new Error(msg + err.annotate());
    }

    self.hashKey    = data.hashKey;
    self.rangeKey   = data.rangeKey;
    self.tableName  = data.tableName;
    self.timestamps = data.timestamps;

    if(data.indexes) {
      self.globalIndexes    = _.chain(data.indexes).filter({ type: 'global' }).indexBy('name').value();
      self.secondaryIndexes = _.chain(data.indexes).filter({ type: 'local' }).indexBy('name').value();
    }

    if(data.schema) {
      self._modelSchema    = _.isPlainObject(data.schema) ? Joi.object().keys(data.schema) : data.schema;
    } else {
      self._modelSchema = Joi.object();
    }

    if(self.timestamps) {
      var extended = self._modelSchema.keys({
        createdAt : Joi.date(),
        updatedAt : Joi.date()
      });

      self._modelSchema = extended;
    }

    self._modelDatatypes = internals.parseDynamoTypes(self._modelSchema.describe());
  });
};

Schema.types = {};

Schema.types.stringSet = function () {
  var set = Joi.array().includes(Joi.string()).meta({dynamoType : 'SS'});

  return set;
};

Schema.types.numberSet = function () {
  var set = Joi.array().includes(Joi.number()).meta({dynamoType : 'NS'});
  return set;
};

Schema.types.binarySet = function () {
  var set = Joi.array().includes(Joi.binary(), Joi.string()).meta({dynamoType : 'BS'});
  return set;
};

Schema.types.uuid = function () {
  return Joi.string().guid().default(nodeUUID.v4);
};

Schema.types.timeUUID = function () {
  return Joi.string().guid().default(nodeUUID.v1);
};

Schema.prototype.validate = function (params, options) {
  options = options || {};

  return Joi.validate(params, this._modelSchema, options);
};

internals.invokeDefaultFunctions = function (data) {
  return _.mapValues(data, function (val) {
    if(_.isFunction(val)) {
      return val.call(null);
    } else if (_.isPlainObject(val)) {
      return internals.invokeDefaultFunctions(val);
    } else {
      return val;
    }
  });
};

Schema.prototype.applyDefaults = function (data) {
  var result = this.validate(data, {abortEarly : false});

  return internals.invokeDefaultFunctions(result.value);
};

Schema.prototype.createMapAlias = function(config) {
  var obj = {};
  var self = this;
  _.each(config, function(c, k) {
    if (_.isPlainObject(c)) {
      if (!c.type) {
        obj[c.name] = {};
        obj[c.name].value = k;
        obj[c.name].content = self.createMapAlias(c);
      }
      else if (c.include) {
        obj[c.name] = [];
        obj[c.name].value = k;
        obj[c.name].content = self.createMapAlias(c.include);
      }
      else {
        obj[c.name] = {};
        obj[c.name].value = k;
      }
    }
  });
  return obj;
};

Schema.prototype.createMapName = function(config) {
  var obj = {};
  var self = this;
  _.each(config, function(c, k) {
    if (_.isPlainObject(c)) {
      if (!c.type) {
        obj[k] = {};
        obj[k].value = c.name;
        obj[k].content = self.createMapName(c);
      }
      else if (c.include) {
        obj[k] = [];
        obj[k].value = c.name;
        obj[k].content = self.createMapName(c.include);
      }
      else {
        obj[k] = {};
        obj[k].value = c.name;
      }
    }
  });
  return obj;
};

Schema.prototype.convertToName = function(config,map){
  var obj = {};
  map = map || this._mapName;
  var self = this;
  _.each(config,function(c, k) {
    if (_.isPlainObject(c)) {
      if (!c.type) {
        obj[c.name] = self.convertToName(c,map[k]);
      }
      else {
        obj[c.name] = c.type;
      }
    }
  });
  return obj;
};

Schema.prototype.serializeItem = function(config,map){
  var obj = {};
  map = map || this._mapName;
  var self = this;
  _.each(config,function(c, k) {
    var key = map[k];
      if (key) {
        if(_.isPlainObject(c)){
          obj[key.value] = self.serializeItem(c,map[k].content);
        }
        else if(_.isArray(map[k])){
          obj[key.value] = [];
          _.each(c,function(c) {
              obj[key.value].push(self.serializeItem(c,map[k].content));
          });
        }
        else{
          obj[key.value] = c;
        }
      }
  });
  return obj;
};

Schema.prototype.deserializeItem = function(config,map){
  var obj = {};
  map = map || this._mapAlias;
  var self = this;
  _.each(config,function(c, k) {
    var key = map[k];
      if (key) {
        if(_.isPlainObject(c)){
          obj[key.value] = self.deserializeItem(c,map[k].content);
        }
        else{
          obj[key.value] = c;
        }
      }
  });
  return obj;
};