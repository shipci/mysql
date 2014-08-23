/**
 * mio-mysql tests.
 */

var should = require('should');
var mio = require('mio');
var mysql = process.env.JSCOV ? require('../lib-cov/mysql') : require('../lib/mysql');

var settings = {};

mysql.mysql.createConnection = function(settings) {
  return {
    config: {},
    connect: function(cb) {
      cb();
    },
    on: function(event, callback) {},
    query: function(statement, done) {
      done && done(null, [], {});
    }
  };
};

describe('module', function(done) {
  it('exports plugin factory', function(done) {
    should.exist(mysql);
    mysql.should.have.type('function');
    done();
  });

  it('constructs new mysql plugins', function(done) {
    var plugin = mysql(settings);
    should.exist(plugin);
    plugin.should.have.type('function');
    done();
  });
});

describe('plugin', function() {
  it('exposes db connection on Model', function(done) {
    var User = mio.createModel('User').attr('id').attr('name');
    User.use(mysql(settings));
    should.exist(User.options.mysql.db);
    done();
  });

  it('destroys connection on error', function(done) {
    var handler = {};
    mysql.mysql.createPool = function(settings) {
      return {
        emit: function(event, obj) {
          if (handler[event]) handler[event](obj);
        },
        on: function(event, callback) {
          handler[event] = callback;
        }
      };
    };
    var User = mio.createModel('User').attr('id').attr('name');
    User.use(mysql({}));
    User.options.mysql.pool.emit('connection', {
      on: function(event, obj) {
        handler[event] = obj;
      },
      emit: function() {
        if (handler[event]) handler[event](obj);
      },
      config: {},
      destroy: function() {
        done();
      },
      query: function() {
        var error = new Error('connection lost');
        error.code = 'PROTOCOL_CONNECTION_LOST';
        User.options.mysql.pool.emit('error', error);
      }
    });
  });
});

describe('adapter', function() {
  var User, Post, Tag;

  beforeEach(function(done) {
    settings = {};

    mysql.mysql.createPool = function() {
      var pool = {};
      pool.query = function() {
        var args = Array.prototype.slice.call(arguments);
        var cb = args.pop();
        cb();
      };
      pool.emit = function() {};
      pool.on = function() {};
      pool.getConnection = function(cb) {
        cb(null, {
          destroy: function() {},
          query: pool.query,
          release: function() { }
        });
      };

      return pool;
    };
    var TagUser = mio.createModel('TagUser').attr('user_id').attr('tag_id');
    User = mio.createModel('User')
      .attr('id', { primary: true })
      .attr('name')
      .attr('subscribed_at', { type: 'date', columnType: 'datetime' })
      .attr('updated_at', { type: 'date', columnType: 'integer' })
      .attr('created_at', { type: 'date', columnType: 'timestamp' });
    Post = mio.createModel('Post').attr('id', { primary: true }).attr('title').attr('user_id');
    Tag = mio.createModel('Tag').attr('id', { primary: true })
    User.use(mysql(settings));
    Post.use(mysql(settings));
    Tag.use(mysql(settings));
    TagUser.use(mysql(settings));
    done();
  });

  describe('.findAll()', function() {
    it('finds all models successfully', function(done) {
      var userA = new User({id: 1, name: 'alex'});
      var userB = new User({id: 2, name: 'jeff'});
      var query = User.options.mysql.pool.query;
      User.options.mysql.pool.query = function(statement, callback) {
        for (var key in userA.attributes) {
          userA.attributes[User.options.mysql.tableName + '_' + key] = userA.attributes[key];
        }
        for (var key in userB.attributes) {
          userB.attributes[User.options.mysql.tableName + '_' + key] = userB.attributes[key];
        }
        callback(null, [userA.attributes, userB.attributes], userB.attributes);
      };
      User.all(
        { $or: { id: userA.primary, name: "jeff" }},
        function(err, found) {
          User.options.mysql.pool.query = query;
          if (err) return done(err);
          should.exist(found);
          found.should.be.instanceOf(Array);
          found.pop().primary.should.equal(userB.primary);
          done();
        }
      );
    });

    it('finds models with foreign key of relation', function(done) {
      var user = new User({id: 1, name: 'alex'});
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        cb(null, [user.attributes], user.attributes);
      };
      User.findAll({ tag_id: 5 }, function(err, collection) {
        if (err) return done(err);
        done();
      });
    });

    it('supports limit and offset pagination parameters', function(done) {
      var userA = new User({id: 1, name: 'alex'});
      var userB = new User({id: 2, name: 'jeff'});
      var query = User.options.mysql.pool.query;
      User.options.mysql.pool.query = function(statement, callback) {
        for (var key in userA.attributes) {
          userA.attributes[User.options.mysql.tableName + '_' + key] = userA.attributes[key];
        }
        for (var key in userB.attributes) {
          userB.attributes[User.options.mysql.tableName + '_' + key] = userB.attributes[key];
        }
        callback(null, [userA.attributes, userB.attributes], userB.attributes);
      };
      User.all(
        { $or: { id: userA.primary, name: "jeff" }, limit: 25, offset: 75 },
        function(err, found) {
          if (err) return done(err);
          should.exist(found);
          found.should.be.instanceOf(Array);
          found.should.have.property('limit', 25);
          found.should.have.property('offset');
          done();
        }
      );
    });

    it('supports page and pageSize pagination parameters', function(done) {
      var userA = new User({id: 1, name: 'alex'});
      var userB = new User({id: 2, name: 'jeff'});
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, callback) {
        for (var key in userA.attributes) {
          userA.attributes[User.options.mysql.tableName + '_' + key] = userA.attributes[key];
        }
        for (var key in userB.attributes) {
          userB.attributes[User.options.mysql.tableName + '_' + key] = userB.attributes[key];
        }
        var collection = [userA.attributes, userB.attributes];
        collection.length = 107;
        callback(null, collection, userB.attributes);
      };
      User.all(
        { $or: { id: userA.primary, name: "jeff" }, page: 4, pageSize: 25 },
        function(err, found) {
          if (err) return done(err);
          should.exist(found);
          found.should.be.instanceOf(Array);
          found.should.have.property('page', 4);
          found.should.have.property('pages', 5);
          found.should.have.property('pageSize', 25);
          done();
        }
      );
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, callback) {
        callback(new Error('error finding users.'));
      };
      User.all(
        { where: { $or: { id: user.primary, name: "alex" }}},
        function(err, found) {
          User.options.mysql.db.query = query;
          should.exist(err);
          err.should.have.property('message', 'error finding users.');
          done();
        }
      );
    });

    it("uses attribute definition's columnName in queries", function(done) {
      User = mio.createModel('User')
        .attr('id', { primary: true })
        .attr('fullname', {
          type: 'string',
          length: 255,
          columnName: 'name'
        });
      User.use(mysql(settings));
      var user = new User({ fullname: 'alex' });
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        User.options.mysql.db.query = query;
        statement.sql.should.equal(
          'insert into "user" ("name") values (\'alex\')'
        );
        cb(null, { insertId: 1 }, {});
      };
      user.save(function(err) {
        if (err) return done(err);
        user.should.have.property('fullname');
        user.fullname.should.equal('alex');
        done();
      });
    });

    it("transforms column names in results", function(done) {
      User = mio.createModel('User')
        .attr('id', { primary: true })
        .attr('fullname', {
          type: 'string',
          length: 255,
          columnName: 'name'
        })
        .use(mysql(settings));

      var query = User.options.mysql.db.query;

      User.options.mysql.pool.query = function(statement, cb) {
        cb(null, [{ id: 1, name: 'alex' }], {});
      };

      User.findOne(1, function(err, user) {
        if (err) return done(err);
        user.should.have.property('fullname');
        user.fullname.should.equal('alex');
        done();
      });
    });

    it('transforms uuid values in results', function(done) {
      User = mio.createModel('User')
        .attr('id', {
          type: 'uuid',
          primary: true
        })
        .use(mysql(settings));

      var query = User.options.mysql.db.query;

      User.options.mysql.pool.query = function(statement, cb) {
        cb(null, [{ user_id: new Buffer('110E8400E29B11D4A716446655440000', 'hex') }], {});
      };

      User.findOne('110E8400-E29B-11D4-A716-446655440000', function(err, user) {
        if (err) return done(err);
        user.should.have.property('id', ('110E8400-E29B-11D4-A716-446655440000').toLowerCase());
        done();
      });
    });

    it('transforms boolean values in results', function(done) {
      User = mio.createModel('User')
        .attr('id', { primary: true })
        .attr('active', {
          type: 'boolean'
        })
        .use(mysql(settings));

      var query = User.options.mysql.db.query;

      User.options.mysql.pool.query = function(statement, cb) {
        cb(null, [{ user_id: 1, user_active: 1 }], {});
      };

      User.findOne(1, function(err, user) {
        if (err) return done(err);
        user.should.have.property('active', true);
        done();
      });
    });
  });

  describe('.findOne()', function() {
    it('finds model by id successfully', function(done) {
      var user = new User({id: 1, name: 'alex'});
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        statement.sql.should.equal(
          'select "user".* from "user" where "user"."id" = 1'
        );
        User.options.mysql.db.query = query;
        for (var key in user.attributes) {
          user.attributes[User.options.mysql.tableName + '_' + key] = user.attributes[key];
        }
        cb(null, [user.attributes], {});
      };
      User.findOne(user.primary, function(err, found) {
        if (err) return done(err);
        should.exist(found);
        user.primary.should.equal(found.primary);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, callback) {
        callback(new Error('error finding user.'));
      };
      User.findOne(user.primary, function(err, found) {
        User.options.mysql.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error finding user.');
        done();
      });
    });
  });

  describe('.count()', function() {
    it('counts models successfully', function(done) {
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        statement.sql.should.equal(
          'select COUNT(*) as _count from "user" where "user"."name" = \'alex\''
        );
        cb(null, [{__count: 3}], {});
      };
      User.count({ name: 'alex' }, function(err, count) {
        User.options.mysql.db.query = query;
        if (err) return done(err);
        should.exist(count);
        count.should.equal(3);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        cb(new Error("error removing all models."));
      };
      User.count({ name: 'alex' }, function(err) {
        User.options.mysql.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error removing all models.');
        done();
      });
    });
  });

  describe('.save()', function() {
    it('saves new model successfully', function(done) {
      var user = new User({name: 'alex'});
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        User.options.mysql.db.query = query;
        cb(null, { insertId: 1 }, {});
      };
      user.save(function(err) {
        should.not.exist(err);
        should.exist(user.primary);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, callback) {
        User.options.mysql.db.query = query;
        callback(new Error('error saving user.'));
      };
      user.save(function(err) {
        should.exist(err);
        err.should.have.property('message', 'error saving user.');
        done();
      });
    });
  });

  describe('.update()', function() {
    it('updates model successfully', function(done) {
      var user = new User({id: 1, name: 'alex'});
      user.dirtyAttributes.length = 0;
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        User.options.mysql.db.query = query;
        statement.sql.should.equal(
          'update "user" set "name" = \'jeff\' where "user"."id" = 1'
        );
        cb(null, [user], user.attributes);
      };
      user.name = 'jeff';
      user.save(function(err) {
        should.not.exist(err);
        user.name.should.equal('jeff');
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ name: 'alex' });
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, callback) {
        callback(new Error('error updating user.'));
      };
      user.save(function(err) {
        User.options.mysql.db.query = query;
        should.exist(err);
        err.should.have.property('message', 'error updating user.');
        done();
      });
    });
  });

  describe('.remove()', function() {
    it('removes model successfully', function(done) {
      var user = new User({id: 1, name: 'alex'});
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        User.options.mysql.db.query = query;
        statement.sql.should.equal(
          'delete from "user" where "user"."id" = 1'
        );
        cb(null, [], {});
      };
      user.remove(function(err) {
        should.not.exist(err);
        done();
      });
    });

    it('passes errors to callback', function(done) {
      var user = new User({ id: 1, name: 'alex' });
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, callback) {
        User.options.mysql.db.query = query;
        callback(new Error('error removing user.'));
      };
      user.remove(function(err) {
        should.exist(err);
        err.should.have.property('message', 'error removing user.');
        done();
      });
    });
  });

  describe('.query()', function() {
    it('retries on deadlock', function(done) {
      User.options.mysql.pool.getConnection = function(cb) {
        count = 0;

        cb(null, {
          release: function() {},
          query: query = function(s, cb) {
            count++;
            if (count > 2) {
              return cb(null, [{ user_id: 1 }], { id: 1 });
            }
            cb(new Error('DEADLOCK'));
          }
        });
      };
      User.findOne(1, function(err, user) {
        if (err) return done(err);
        should.exist(user);
        user.should.have.property('id', 1);
        done();
      });
    });

    it('converts date attributes to proper format', function(done) {
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        cb(null, [{ user_id: 1 }], { id: 1 });
      };
      User.findAll({
        subscribed_at: {
          $gt: 2012,
          $lt: '2013-05'
        },
        created_at: {
          $gt: '2012',
          $lt: new Date()
        },
        updated_at: {
          $gt: 1388346754
        }
      },
      function(err, collection) {
        if (err) return done(err);
        done();
      });
    });

    it('converts boolean attributes to 1 or 0', function(done) {
      var query = User.options.mysql.db.query;
      User.options.mysql.db.query = function(statement, cb) {
        User.options.mysql.db.query = function(statement, cb) {
          statement.sql.should.include('active" = ?');
          statement.sql.should.include('flagged" = ?');
          values.should.include(1);
          values.should.include(0);
          cb(null, [{ user_id: 1 }], { id: 1 });
        };
        cb(null, [{ user_id: 1 }], { id: 1 });
      };
      User.findAll({ active: true, flagged: false }, function(err, collection) {
        if (err) return done(err);
        done();
      });
    });
  });

  describe('collection', function() {
    describe('#toJSON()', function() {
      it('includes pagination properties', function(done) {
        var query = User.options.mysql.db.query;
        User.options.mysql.db.query = function(statement, cb) {
          User.options.mysql.db.query = function(statement, cb) {
            cb(null, [{ user_id: 1 }], { id: 1 });
          };
          cb(null, [{ user_id: 1 }], { id: 1 });
        };
        User.findAll({ created_at: 1234567890 }, function(err, collection) {
          if (err) return done(err);
          var json = collection.toJSON();
          json.should.have.property('collection');
          collection.should.have.property('length', 1);
          json.should.have.property('offset', 0);
          json.should.have.property('limit', 50);
          done();
        });
      });
    });
  });
});
