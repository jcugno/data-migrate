var mysql = require('mysql')
  , _ = require('lodash')
  , async = require('async');


var cfg = {};
require('rc')('data-migrate', cfg);

function Migrator(cfg) {
  this.cfg = cfg;
}

module.exports = function() {
  var migrator = new Migrator(cfg);

  migrator.migrate();
};

Migrator.prototype.migrate = function() {

  // init
  // get tables
  // for each table from source get the last ID from the target
  // Get the delta data from source ID to target Latest id
  // import in the new data
  async.waterfall(
    [
      this.initConnections.bind(this),
      this.verifyTables.bind(this),
      this.migrateTables.bind(this)
    ],
    function(err, result) {
      if (err) { console.error(err); }

      console.log("WATERFALL DONE");
      process.exit(0);
    }
  );
};

Migrator.prototype._getTables = function(results) {

  return _.pluck(results, 'Tables_in_' + this.cfg.database);
};

Migrator.prototype.verifyTables = function(connections, cb) {

  var source = connections.source,
  target = connections.target,
  self = this;

  source.query('SHOW TABLES', function(err, results) {
    if (err) { return cb(err); }

    var sourceTables = self._getTables(results);

    target.query('SHOW TABLES', function(err, results) {
      var targetTables = self._getTables(results);

      var diff = _.difference(sourceTables, targetTables);

      if (diff.length !== 0) {
        //return cb("The tables in source and target do not match");
      }

      cb(null, connections, targetTables);
    });

  });

};

Migrator.prototype.migrateTables = function(connections, tables, cb) {

  tables.forEach(function(table) {
 
    connections.target.query('SELECT id FROM ' + table + ' ORDER BY id DESC LIMIT 1', function (err, result) {

      if (err) { 
        console.log(table);
        console.log(err);
      }

    });

  });

};

// Start the migration process
Migrator.prototype.initConnections = function(externalCallback) {

  this.source = mysql.createConnection({
      host: cfg.sourceHost,
      user: cfg.sourceUser,
      password: cfg.sourcePassword,
      database: cfg.database
  });

  this.target = mysql.createConnection({
      host: cfg.targetHost,
      user: cfg.targetUser,
      password: cfg.targetPassword,
      database: cfg.database
  });

  var self = this;
  async.parallel({

      // Connect to both DBs
      source: function(cb) {
        self.source.connect(function(err) {
          cb(err, self.source);
        });
      },

      target: function(cb) {
        self.target.connect(function(err) {
          cb(err, self.target);
        });
      }
  }, externalCallback);
};

