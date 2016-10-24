'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const discover = require('mongodb-schema');
const {JugglerAccessor} = require('lyda-accessor');
const debug = require('debug')('lyda:accessor:mongodb');

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

  aggregate(query, options) {
    if (!this.ds.connected) {
      throw new Error('Connection is unconnected, `aggregate` should run after connect.');
    }


    options = options || {};
    const {db} = this.connector;

    // transform filters to mongodb $match style
    const filters = transformFilters(query.filters);

    // // pick fields should be includes
    // const fields = {};
    // _.forEach(query.fields, field => (fields[field.columnName] = 1));
    // _.forEach(query.additionals, column => (fields[column.name] = 1));


    // transform sorts
    const sort = {};
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
        }
      } else {
        sort['_id.' + order.name] = order.direction;
      }
    });


    // setup mongodb aggregate pipeline
    const project = {}, group = {}, fields = {};
    let match = filters;

    if (query.where) {
      if (match) {
        match = {$and: [query.where, match]}
      } else {
        match = query.where;
      }
    }

    // fields and project
    _.forEach(query.fields, field => {
      const {name, columnName, aggregation} = field;

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
      } else {
        project[name] = '$' + columnName;
      }
    });

    _.forEach(query.additionals, column => {
      if (!project[column.name]) {
        project[column.name] = '$' + column.columnName;
      }
      if (!fields[column.name]) {
        fields[column.name] = '$' + column.name;
      }
    });

    group['_id'] = fields;

    const aggregation = [];
    if (match) {
      aggregation.push({$match: match});
    }
    if (!_.isEmpty(project)) {
      aggregation.push({$project: project});
    }
    aggregation.push({$group: group});
    if (!_.isEmpty(sort)) {
      aggregation.push({$sort: sort});
    }

    // have no joins
    if (_.isEmpty(query.additionals)) {
      if (options.page) {
        aggregation.push({$skip: (options.page - 1) * 100});
        aggregation.push({$limit: 100});
      } else {
        aggregation.push({$limit: 50});
      }
    }

    // debug(aggregation);
    // console.log(JSON.stringify(aggregation, null, '  '));

    // execute aggregate

    const collection = db.collection(query.collection || (query.schema && query.schema.name) || query.name);

    return Promise.fromCallback(callback => collection.aggregate(aggregation, callback)).then(docs => {
      const result = _.map(docs, doc => {
        const item = _.omit(doc, '_id');
        doc._id && Object.assign(item, doc._id);
        return item;
      });

      return result;
    })
  }


};

function transformFilters(filters) {
  const result = _.transform(filters, (result, filter) => {
    let mfilter, {value, operator, columnName, type} = filter;
    if (value || operator === 'notNull' || operator === 'null') {
      if (type === 'number') {
        value = Number(value);
      }
      if (type === 'date') {
        mfilter = Filter.date(columnName, operator, value);
        if (!_.includes(['in', 'nin'], operator)) {
          mfilter = {[columnName]: mfilter};
        }
      } else if (Filter[operator]) {
        mfilter = {[columnName]: Filter[operator](value)};
      }
      if (mfilter) {
        result.push(mfilter);
      }
    }
  }, []);

  if (result.length > 0) {
    return {$and: result};
  }
}
