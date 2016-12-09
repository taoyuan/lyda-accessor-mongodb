'use strict';

const debug = require('debug')('lyda:accessor:mongodb');
const _ = require('lodash');
const util = require('util');
const Promise = require('bluebird');
const discover = require('mongodb-schema');
const {JugglerAccessor} = require('lyda-accessor');
const converter = require('exco-mongodb');

const Filter = require('./filter');

const typeMapper = {};

module.exports = class MongoDB extends JugglerAccessor {

  constructor(settings) {
    settings = _.assign({connector: require('loopback-connector-mongodb')}, settings);
    super('mongodb', settings);
  }

  connect() {
    return Promise.fromCallback(cb => this.ds.connect(cb)).then(() => this.ds.connector.db);
  }

  disconnect() {
    return Promise.fromCallback(cb => this.ds.disconnect(cb));
  }

  discoverResources(options) {
    return this.connect().then(db => {
      return Promise.fromCallback(callback => db.listCollections().toArray(callback))
        .then(defs => _.filter(defs, def => !def.name.startsWith('system.')).map(def => {
          return {name: def.name, title: def.name}
        }));
    })

  }

  discoverSchemas(resourceName, options) {

    return this.connect().then(db => {
      return Promise.fromCallback(cb => discover(db.collection(resourceName).find().limit(20), cb)).then(descriptor => {
        const schema = {
          name: resourceName,
          properties: {}
        };

        _.forEach(descriptor.fields, field => {
          const name = field.name === '_id' ? 'id' : field.name;

          // TODO type mapper
          const prop = {
            name: name,
            type: field.type,
            label: name
          };
          if (Array.isArray(field.type)) {
            prop.type = 'any';
          }
          if (field.type === 'Document') {
            prop.type = 'Object'
          }
          schema.properties[name] = prop;
        });
        return schema;
      });
    });
  }

  buildAggregation(query, options) {

    options = options || {};

    // setup mongodb aggregate pipeline
    const project = {}, group = {}, fields = {};

    // TODO parse where and add key to project
    if (query.where) {
      _.forEach(query.where, (cond, name) => {
        project[name] = '$' + name;
      });
    }

    _.forEach(query.filters, filter => {
      const {name, columnName, expression} = filter;
      if (columnName) {
        project[name] = '$' + columnName;
      } else if (expression) {
        project[name] = converter.convert(expression, options);
      }
    });

    // fields -> group and project
    if (!_.isEmpty(query.fields)) {
      _.forEach(query.fields, field => {
        const {name, columnName, expression, aggregation} = field;

        if (!columnName && !expression) return;

        if (aggregation) { // measures
          if (_.includes(['sum', 'avg', 'min', 'max'], aggregation)) {
            group[name + '_' + aggregation] = {['$' + aggregation]: '$' + name};
          } else if (aggregation === 'count') {
            group[name + '_' + 'count'] = {$sum: 1};
          } else if (_.includes(['year', 'month', 'day'], aggregation)) {
            project[name + '_' + aggregation] = {['$' + aggregation]: '$' + columnName};
            fields[name + '_' + aggregation] = '$' + name + '_' + aggregation;
          }
        } else { // dims
          fields[name] = '$' + name;
        }


        if (field.fn && _.includes(['toUpper', 'toLower'], field.fn)) {
          project[name] = {['$' + field.fn]: '$' + columnName};
        } else if (columnName) {
          project[name] = '$' + columnName;
        } else if (expression) {
          project[name] = converter.convert(expression, options);
        }
      });
    }

    if (!_.isEmpty(query.additions)) {
      _.forEach(query.additions, field => {
        if (!project[field.name]) {
          project[field.name] = '$' + field.columnName;
        }
        if (!fields[field.name]) {
          fields[field.name] = '$' + field.name;
        }
      });
    }

    // transform sorts
    const sort = {};
    if (!_.isEmpty(query.orders)) {
      _.forEach(query.orders, order => {
        const field = _.find(query.fields, field => field.name === order.name);
        if (field) {
          const {aggregation} = field;
          if (aggregation) {
            if (_.includes(['sum', 'avg', 'min', 'max', 'count'], aggregation)) {
              sort[order.name + '_' + aggregation] = order.direction;
            } else if (_.includes(['year', 'month', 'day'], aggregation)) {
              sort['_id.' + order.name + '_' + aggregation] = order.direction;
            }
          } else {
            sort['_id.' + order.name] = order.direction;
          }
        } else {
          sort['_id.' + order.name] = order.direction;
        }
      });
    }

    // transform filters to mongodb $match style
    // const filters = !_.isEmpty(query.filters) && transformFilters(query.filters);

    let match = !_.isEmpty(query.filters) && transformFilters(query.filters);
    if (query.where) {
      if (match) {
        match = {$and: [query.where, match]}
      } else {
        match = query.where;
      }
    }

    group['_id'] = fields;

    const aggregation = [];

    if (!_.isEmpty(project)) {
      aggregation.push({$project: project});
    }

    if (match) {
      aggregation.push({$match: match});
    }
    aggregation.push({$group: group});
    if (!_.isEmpty(sort)) {
      aggregation.push({$sort: sort});
    }

    // have no joins
    if (_.isEmpty(query.additions)) {
      if (options.limit || options.skip) {
        aggregation.push({$skip: options.skip || 0});
        aggregation.push({$limit: options.limit || 100});
      } else if (options.page) {
        aggregation.push({$skip: (options.page - 1) * 100});
        aggregation.push({$limit: 100});
      } else {
        aggregation.push({$limit: 50});
      }
    }

    // debug(aggregation);
    // console.log(JSON.stringify(aggregation, null, '  '));

    // execute aggregate

    return aggregation;
  }

  aggregate(query, options) {
    if (!this.ds.connected) {
      throw new Error('Connection is unconnected, `aggregate` should run after connect.');
    }

    const aggregation = this.buildAggregation(query, options);
    if (debug.enabled) {
      debug('Built mongo aggregation pipelines:\n%s\n---\n%s',
        JSON.stringify(aggregation),
        util.inspect(aggregation, {colors: true, depth: null}));
    }

    const c = this.connector.db.collection(query.collection || (query.schema && query.schema.name) || query.name);
    return Promise.fromCallback(cb => c.aggregate(aggregation, cb)).then(docs => _.map(docs, doc => {
      const item = _.omit(doc, '_id');
      doc._id && Object.assign(item, doc._id);
      return item;
    }));
  }


};

function transformFilters(filters) {
  const result = _.transform(filters, (result, filter) => {
    let mfilter, {name, value, operator, type} = filter;
    if (value !== undefined || operator === 'notNull' || operator === 'null') {
      if (type === 'number') {
        value = Number(value);
      }
      if (type === 'date') {
        mfilter = Filter.date(name, operator, value);
        if (!_.includes(['in', 'nin'], operator)) {
          mfilter = {[name]: mfilter};
        }
      } else if (Filter[operator]) {
        mfilter = {[name]: Filter[operator](value)};
      }
      if (mfilter) {
        result.push(mfilter);
      }
    }
  }, []);

  if (result.length === 1) {
    return result[0];
  } else if (result.length > 1) {
    return {$and: result};
  }
}

