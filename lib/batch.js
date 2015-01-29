'use strict';

var _ = require('lodash'),
    async = require('async');

var internals = {};

internals.buildInitialGetItemsRequest = function (tableName, keys, options) {
  var request = {};

  request[tableName] = _.merge({}, {Keys : keys}, options);

  return { RequestItems : request };
};

internals.buildInitialDeleteItemsRequest = function (tableName, keys) {
  var request = { };
  request[tableName] = [];
  
  _.each(keys, function(key){
    var DeleteRequest = {};
    DeleteRequest.Key = _.merge({}, key);
    request[tableName].push({ DeleteRequest : DeleteRequest});
  });
  
  return { RequestItems : request };
};

internals.buildInitialPutItemsRequest = function (tableName, keys) {
  var request = { };
  request[tableName] = [];
  
  _.each(keys, function(key){
    var PutRequest = {};
    PutRequest.Item = _.merge({}, key);
    request[tableName].push({ PutRequest : PutRequest});
  });
  
  return { RequestItems : request };
};

internals.serializeKeys = function (keys, table, serializer) {
  return keys.map(function (key) {
    var hashKey;
    var rangeKey;
    
    if(_.isPlainObject(key)){
      if(table.schema._mapAlias){
        key = table.schema.serializeItem(key);
      }
      hashKey = key[table.schema.hashKey];
      rangeKey = key[table.schema.rangeKey];
    }
    else{
      hashKey = key;
    }
    return serializer.buildKey(hashKey, rangeKey, table.schema);
  });
};

internals.serializeItems = function (keys, table) {
  return keys.map(function (key) {
    if(_.isPlainObject(key) && table.schema._mapAlias){
      return table.schema.serializeItem(key);
    }
    return key;
  });
};

internals.mergeResponses = function (tableName, responses) {
  var base = {
    Responses : {},
    ConsumedCapacity : []
  };

  base.Responses[tableName] = [];

  return responses.reduce(function (memo, resp) {
    if(resp.Responses && resp.Responses[tableName]) {
      memo.Responses[tableName] = memo.Responses[tableName].concat(resp.Responses[tableName]);
    }

    return memo;
  }, base);
};

internals.paginatedRequest = function (request, table, callback) {
  var responses = [];

  var doFunc = function (callback) {
    table.runBatchGetItems(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = resp.UnprocessedKeys;
      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null && !_.isEmpty(request);
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.tableName(), responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.paginatedWriteRequest = function (request, table, callback) {
  var responses = [];
  var doFunc = function (callback) {
  
    table.runBatchWriteItem(request, function (err, resp) {
      if(err && err.retryable) {
        return callback();
      } else if(err) {
        return callback(err);
      }

      request = resp.UnprocessedKeys;
      responses.push(resp);

      return callback();
    });
  };

  var testFunc = function () {
    return request !== null && !_.isEmpty(request);
  };

  var resulsFunc = function (err) {
    if(err) {
      return callback(err);
    }

    callback(null, internals.mergeResponses(table.tableName(), responses));
  };

  async.doWhilst(doFunc, testFunc, resulsFunc);
};

internals.buckets = function (keys) {
  var buckets = [];

  while( keys.length ) {
    buckets.push( keys.splice(0, 100) );
  }

  return buckets;
};

internals.initialBatchGetItems = function (keys, table, serializer, options, callback) {
  var serializedKeys = internals.serializeKeys(keys, table, serializer);

  var request = internals.buildInitialGetItemsRequest(table.tableName(), serializedKeys, options);

  internals.paginatedRequest(request, table, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[table.tableName()];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(i));
    });

    return callback(null, items);
  });
};

internals.initialBatchDeleteItems = function (keys, table, serializer, options, callback) {
  var serializedKeys = internals.serializeKeys(keys, table, serializer);

  var request = internals.buildInitialDeleteItemsRequest(table.tableName(), serializedKeys, options);

  internals.paginatedWriteRequest(request, table, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[table.tableName()];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(i));
    });

    return callback(null, items);
  });
};

internals.initialBatchPutItems = function (item, table, serializer, options, callback) {
  var serializedItems = internals.serializeItems(item, table);

  var request = internals.buildInitialPutItemsRequest(table.tableName(), serializedItems, options);

  internals.paginatedWriteRequest(request, table, function (err, data) {
    if(err) {
      return callback(err);
    }

    var dynamoItems = data.Responses[table.tableName()];

    var items = _.map(dynamoItems, function(i) {
      return table.initItem(serializer.deserializeItem(i));
    });

    return callback(null, items);
  });
};

internals.getItems = function (table, serializer) {

  return function (keys, options, callback) {
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }
    
    async.map(internals.buckets(_.clone(keys)), function (key, callback) {
      internals.initialBatchGetItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };

};

internals.deleteItems = function (table, serializer) {

  return function (keys, options, callback) {
    
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(keys)), function (key, callback) {
      internals.initialBatchDeleteItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };
};

internals.putItems = function (table, serializer) {

  return function (keys, options, callback) {
    
    if (typeof options === 'function' && !callback) {
      callback = options;
      options = {};
    }

    async.map(internals.buckets(_.clone(keys)), function (key, callback) {
      internals.initialBatchPutItems(key, table, serializer, options, callback);
    }, function (err, results) {
      if(err) {
        return callback(err);
      }

      return callback(null, _.flatten(results));
    });
  };
};

module.exports = function (table, serializer) {

  return {
    getItems : internals.getItems(table, serializer),
    deleteItems : internals.deleteItems(table,serializer),
    putItems : internals.putItems(table,serializer)
  };

};
