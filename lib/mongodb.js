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
    options = options || {};
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
        if (field.aggregation) {
          if (_.includes(['sum', 'avg', 'min', 'max', 'count'], field.aggregation)) {
            sort[order.columnName + field.aggregation] = order.direction;
          } else if (_.includes(['year', 'month', 'day'], field.aggregation)) {
            sort['_id.' + order.columnName + field.aggregation] = order.direction;
          }
        }
      } else {
        sort['_id.' + order.columnName] = order.direction;
      }
    });


    // setup mongodb aggregate pipeline
    const match = [], project = {}, group = {}, fields = {};

    if (query.match) {
      match.push(..._.castArray(query.match));
    }
    match.push(...filters);

    // fields and project
    _.forEach(query.fields, field => {
      const {columnName, aggregation} = field;

      if (aggregation) { // measures
        if (_.includes(['sum', 'avg', 'min', 'max'], aggregation)) {
          group[columnName + aggregation] = {['$' + aggregation]: '$' + columnName};
        } else if (aggregation === 'count') {
          group[columnName + 'count'] = {$sum: 1};
        } else if (_.includes(['year', 'month', 'day'], aggregation)) {
          project[columnName + aggregation] = {['$' + aggregation]: '$' + columnName};
          fields[columnName + aggregation] = '$' + columnName + aggregation;
        }
      } else { // dims
        fields[columnName] = '$' + columnName;
      }


      if (field.fn && _.includes(['toUpper', 'toLower'], field.fn)) {
        project[columnName] = {['$' + field.fn]: '$' + columnName};
      } else {
        project[columnName] = '$' + columnName;
      }
    });

    _.forEach(query.additionals, column => {
      fields[column.name] = '$' + column.name;
      project[column.name] = '$' + column.name;
    });

    group['_id'] = fields;

    const aggregation = [{$match: match}];
    if (!_.isEmpty(project)) aggregation.push({$project: project});
    aggregation.push({$group: group});
    if (!_.isEmpty(sort)) aggregation.push({$sort: sort});

    // have no joins
    if (_.isEmpty(query.additionals)) {
      if (options.page) {
        aggregation.push({$skip: (options.page - 1) * 100});
        aggregation.push({$limit: 100});
      } else {
        aggregation.push({$limit: 10});
      }
    }

    // debug(aggregation);
    console.log(JSON.stringify(aggregation, null, '  '));

    // TODO connect and aggregate

  }


};

function transformFilters(filters) {
  return _.transform(filters, (result, filter) => {
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
}
