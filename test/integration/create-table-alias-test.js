'use strict';

var vogels = require('../../index'),
  chai = require('chai'),
  expect = chai.expect,
  _ = require('lodash'),
  helper = require('../test-helper'),
  Joi = require('joi');

chai.should();

describe('Create Tables Integration Tests', function() {
  this.timeout(0);

  before(function() {
    vogels.dynamoDriver(helper.realDynamoDB());
  });

  afterEach(function() {
    vogels.reset();
  });

  it('should create table with hash key', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      }
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;
      expect(desc).to.exist;
      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }]);

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with hash and range key', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      rangeKey: 'code',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        code: {
          type: Joi.number(),
          name: 'c'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      }
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }, {
        AttributeName: 'c',
        AttributeType: 'N'
      }]);

      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }, {
        AttributeName: 'c',
        KeyType: 'RANGE'
      }]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with local secondary index', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      rangeKey: 'code',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        code: {
          type: Joi.number(),
          name: 'c'
        },
        id1: {
          type: Joi.string(),
          name: 'i1'
        },
        code1: {
          type: Joi.number(),
          name: 'c1'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      },
      indexes: [{
        hashKey: 'id',
        rangeKey: 'id1',
        type: 'local',
        name: 'Index1'
      }, {
        hashKey: 'id',
        rangeKey: 'code1',
        type: 'local',
        name: 'Index2'
      }, ]
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }, {
        AttributeName: 'c',
        AttributeType: 'N'
      }, {
        AttributeName: 'i1',
        AttributeType: 'S'
      }, {
        AttributeName: 'c1',
        AttributeType: 'N'
      }]);

      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }, {
        AttributeName: 'c',
        KeyType: 'RANGE'
      }]);

      expect(desc.LocalSecondaryIndexes).to.have.length(2);

      expect(_.find(desc.LocalSecondaryIndexes, {
        IndexName: 'Index1'
      })).to.eql({
        IndexName: 'Index1',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, {
          AttributeName: 'i1',
          KeyType: 'RANGE'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        IndexSizeBytes: 0,
        ItemCount: 0
      });

      expect(_.find(desc.LocalSecondaryIndexes, {
        IndexName: 'Index2'
      })).to.eql({
        IndexName: 'Index2',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, {
          AttributeName: 'c1',
          KeyType: 'RANGE'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        IndexSizeBytes: 0,
        ItemCount: 0
      });

      return Model.deleteTable(done);
    });
  });

  it('should create table with local secondary index with custom projection', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      rangeKey: 'code',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        code: {
          type: Joi.number(),
          name: 'c'
        },
        id1: {
          type: Joi.string(),
          name: 'i1'
        },
        code1: {
          type: Joi.number(),
          name: 'c1'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      },
      indexes: [{
        hashKey: 'id',
        rangeKey: 'id1',
        type: 'local',
        name: 'Index1',
        projection: {
          ProjectionType: 'KEYS_ONLY'
        }
      }]
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }, {
        AttributeName: 'c',
        AttributeType: 'N'
      }, {
        AttributeName: 'i1',
        AttributeType: 'S'
      }]);

      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }, {
        AttributeName: 'c',
        KeyType: 'RANGE'
      }]);

      expect(desc.LocalSecondaryIndexes).to.eql([{
        IndexName: 'Index1',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, {
          AttributeName: 'i1',
          KeyType: 'RANGE'
        }, ],
        Projection: {
          ProjectionType: 'KEYS_ONLY'
        },
        IndexSizeBytes: 0,
        ItemCount: 0
      }]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with global index', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      rangeKey: 'code',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        code: {
          type: Joi.number(),
          name: 'c'
        },
        id1: {
          type: Joi.string(),
          name: 'i1'
        },
        code1: {
          type: Joi.number(),
          name: 'c1'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      },
      indexes: [{
        hashKey: 'id',
        type: 'global',
        name: 'GlobalIndex1'
      }]
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }, {
        AttributeName: 'c',
        AttributeType: 'N'
      }]);

      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }, {
        AttributeName: 'c',
        KeyType: 'RANGE'
      }]);

      expect(desc.GlobalSecondaryIndexes).to.eql([{
        IndexName: 'GlobalIndex1',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1
        },
        IndexSizeBytes: 0,
        IndexStatus: 'ACTIVE',
        ItemCount: 0
      }]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with global index with optional settings', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      rangeKey: 'code',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        code: {
          type: Joi.number(),
          name: 'c'
        },
        id1: {
          type: Joi.string(),
          name: 'i1'
        },
        code1: {
          type: Joi.number(),
          name: 'c1'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      },
      indexes: [{
        hashKey: 'id',
        type: 'global',
        name: 'GlobalIndex1',
        projection: {
          NonKeyAttributes: ['wins'],
          ProjectionType: 'INCLUDE'
        },
        readCapacity: 10,
        writeCapacity: 5
      }]
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }, {
        AttributeName: 'c',
        AttributeType: 'N'
      }]);

      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }, {
        AttributeName: 'c',
        KeyType: 'RANGE'
      }]);

      expect(desc.GlobalSecondaryIndexes).to.eql([{
        IndexName: 'GlobalIndex1',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, ],
        Projection: {
          ProjectionType: 'INCLUDE',
          NonKeyAttributes: ['wins']
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 5
        },
        IndexSizeBytes: 0,
        IndexStatus: 'ACTIVE',
        ItemCount: 0
      }]);

      return Model.deleteTable(done);
    });
  });

  it('should create table with global and local indexes', function(done) {
    var Model = vogels.define('vogels-create-table-test', {
      hashKey: 'id',
      rangeKey: 'code',
      useAlias: true,
      tableName: helper.randomName('vogels-createtable-Accounts'),
      schema: {
        id: {
          type: Joi.string(),
          name: 'i'
        },
        code: {
          type: Joi.number(),
          name: 'c'
        },
        id1: {
          type: Joi.string(),
          name: 'i1'
        },
        code1: {
          type: Joi.number(),
          name: 'c1'
        },
        obj: {
          name: 'o',
          sousObj1: {
            type: Joi.string(),
            name: 'so1'
          },
          sousObj2: {
            name: 'so2',
            sousSousObj1: {
              type: Joi.string(),
              name: 'ss01'
            }
          }
        }
      },
      indexes: [{
        hashKey: 'id',
        type: 'global',
        name: 'GlobalIndex1'
      }, {
        hashKey: 'code1',
        rangeKey: 'code',
        type: 'global',
        name: 'GlobalIndex2'
      }, {
        hashKey: 'id',
        rangeKey: 'id1',
        type: 'local',
        name: 'LocalIndex1'
      }, {
        hashKey: 'id',
        rangeKey: 'code1',
        type: 'local',
        name: 'LocalIndex2'
      }]
    });

    Model.createTable(function(err, result) {
      expect(err).to.not.exist;

      var desc = result.TableDescription;

      expect(desc).to.exist;

      expect(desc.AttributeDefinitions).to.eql([{
        AttributeName: 'i',
        AttributeType: 'S'
      }, {
        AttributeName: 'c',
        AttributeType: 'N'
      }, {
        AttributeName: 'i1',
        AttributeType: 'S'
      }, {
        AttributeName: 'c1',
        AttributeType: 'N'
      }]);

      expect(desc.KeySchema).to.eql([{
        AttributeName: 'i',
        KeyType: 'HASH'
      }, {
        AttributeName: 'c',
        KeyType: 'RANGE'
      }]);

      expect(desc.GlobalSecondaryIndexes).to.have.length(2);

      expect(_.find(desc.GlobalSecondaryIndexes, {
        IndexName: 'GlobalIndex1'
      })).to.eql({
        IndexName: 'GlobalIndex1',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1
        },
        IndexSizeBytes: 0,
        IndexStatus: 'ACTIVE',
        ItemCount: 0
      });

      expect(_.find(desc.GlobalSecondaryIndexes, {
        IndexName: 'GlobalIndex2'
      })).to.eql({
        IndexName: 'GlobalIndex2',
        KeySchema: [{
          AttributeName: 'c1',
          KeyType: 'HASH'
        }, {
          AttributeName: 'c',
          KeyType: 'RANGE'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1
        },
        IndexSizeBytes: 0,
        IndexStatus: 'ACTIVE',
        ItemCount: 0
      });

      expect(desc.LocalSecondaryIndexes).to.have.length(2);

      expect(_.find(desc.LocalSecondaryIndexes, {
        IndexName: 'LocalIndex1'
      })).to.eql({
        IndexName: 'LocalIndex1',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, {
          AttributeName: 'i1',
          KeyType: 'RANGE'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        IndexSizeBytes: 0,
        ItemCount: 0
      });

      expect(_.find(desc.LocalSecondaryIndexes, {
        IndexName: 'LocalIndex2'
      })).to.eql({
        IndexName: 'LocalIndex2',
        KeySchema: [{
          AttributeName: 'i',
          KeyType: 'HASH'
        }, {
          AttributeName: 'c1',
          KeyType: 'RANGE'
        }, ],
        Projection: {
          ProjectionType: 'ALL'
        },
        IndexSizeBytes: 0,
        ItemCount: 0
      });

      return Model.deleteTable(done);
    });
  });
});
