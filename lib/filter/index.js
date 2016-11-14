"use strict";

const _ = require('lodash');

const Filter = module.exports = {};

Filter.date = (name, operator, value) => {
  return require('./date')(name, operator, value);
};

Filter.eq = Filter.equal = value => value;
Filter.ne = value => ({$ne: value});

Filter.not = value => ({$not: value});

Filter.gt = Filter.greaterThan = Filter.biggerThan = value => ({$gt: value});
Filter.gte = Filter.greaterOrEqualThan = Filter.biggerOrEqualThan = value => ({$gte: value});
Filter.ngt = Filter.notGreaterThan = value => ({$not: {$gt: value}});

Filter.lt = value => Filter.lessThan = ({$lt: value});
Filter.lte = value => Filter.lessOrEqualThan = ({$lte: value});
Filter.nlt = Filter.notLessThan = value => ({$not: {lt: value}});

Filter.between = (...values) => {
  values = _.flatten(values);
  if (values.length < 2) throw new Error('`between` requires two value at least!');
  return {$gt: values[0], $lt: values[1]};
};

Filter.notBetween = (...values) => {
  values = _.flatten(values);
  if (values.length < 2) throw new Error('`notBetween` requires two value at least!');
  return {$not: {$gt: values[0], $lt: values[1]}};
};

Filter.contains = value => (new RegExp(value, 'i'));
Filter.notContains = value => ({$ne: new RegExp(value, 'i')});

Filter.startWith = value => (new RegExp('/^' + value + '/', 'i'));
Filter.notStartWith = value => ({$ne: new RegExp('/^' + value + '/', 'i')});

Filter.endWith = value => (new RegExp('/' + value + '$/', 'i'));
Filter.notEndWith = value => ({$ne: new RegExp('/' + value + '$/', 'i')});

Filter.like = value => (new RegExp('/' + value + '/', 'i'));
Filter.nlike = value => ({$ne: new RegExp('/' + value + '/', 'i')});

Filter.null = () => null;
Filter.notNull = () => ({$ne: null});

Filter.in = value => ({$in: String(value).split(';')});
Filter.nin = Filter.notIn = value => ({$nin: String(value).split(';')});


