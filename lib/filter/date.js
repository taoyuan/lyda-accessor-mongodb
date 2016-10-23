"use strict";

const _ = require('lodash');
const moment = require('moment');

const micros = {}, specs = {};

const regmicro = /^\#([\w_]*)/i;

module.exports = exports = function (name, operator, value) {
  operator = operator.toLowerCase();
  if (specs[operator]) {
    const values = exports.resolve(value);
    return specs[operator](values, name);
  }
};

exports.micros = micros;
exports.specs = specs;

exports.resolve = function (value) {
  if (typeof value === 'string' && regmicro.test(value)) {
    const micro = regmicro.exec(value)[1].toLowerCase();
    if (micros[micro]) {
      value = micros[micro]();
    }
  }
  return Array.isArray(value) ? value : [value];
};

micros.today = () => {
  const d = moment();
  return [d.startOf('day').toDate(), d.endOf('day').toDate()];
};

micros.yesterday = () => {
  const d = moment().subtract(1, 'days');
  return [d.startOf('day').toDate(), d.endOf('day').toDate()];
};

micros.this_week = () => {
  // first day monday instead sunday
  const d = moment().subtract(1, 'days');
  return [d.startOf('week').add(1, 'days').toDate(), d.endOf('week').add(1, 'days').toDate()];
};

micros.last_week = () => {
  // first day monday instead sunday
  const d = moment().subtract(8, 'days');
  return [d.startOf('week').add(1, 'days').toDate(), d.endOf('week').add(1, 'days').toDate()];
};

micros.this_month = () => {
  const d = moment();
  return [d.startOf('month').toDate(), d.endOf('month').toDate()];
};

micros.last_month = () => {
  const d = moment().subtract(1, 'months');
  return [d.startOf('month').toDate(), d.endOf('month').toDate()];
};

micros.this_year = () => {
  const d = moment();
  return [d.startOf('year').toDate(), d.endOf('year').toDate()];
};

micros.last_year = () => {
  const d = moment().subtract(1, 'years');
  return [d.startOf('year').toDate(), d.endOf('year').toDate()];
};

micros.first_quarter = () => {
  const d = moment().month(1).date(1);
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.second_quarter = () => {
  const d = moment().month(4).date(1);
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.third_quarter = () => {
  const d = moment().month(7).date(1);
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.forth_quarter = () => {
  const d = moment().month(10).date(1);
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.ly_first_quarter = () => {
  const d = moment().month(1).date(1).subtract(1, 'years');
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.ly_second_quarter = () => {
  const d = moment().month(4).date(1).subtract(1, 'years');
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.ly_third_quarter = () => {
  const d = moment().month(7).date(1).subtract(1, 'years');
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

micros.ly_forth_quarter = () => {
  const d = moment().month(10).date(1).subtract(1, 'years');
  return [d.startOf('quarter').toDate(), d.endOf('quarter').toDate()];
};

/////////////////////////////////////////////////////////
// specs
/////////////////////////////////////////////////////////
specs.eq = values => ({$gte: moment(values[0]).toDate(), $lt: moment(values[1]).add(1, 'days').toDate()});
specs.neq = values => ({$not: {$gte: moment(values[0]).toDate(), $lt: moment(values[1]).add(1, 'days').toDate()}});

specs.gt = values => ({$gt: moment(values[0]).toDate()});
specs.gte = values => ({$gte: moment(values[0]).toDate()});
specs.ngt = values => ({$not: {$gt: moment(values[0]).add(1, 'days').toDate()}});

specs.lt = values => ({$lt: moment(values[0]).toDate()});
specs.lte = values => ({$lte: moment(values[0]).add(1, 'days').toDate()});
specs.nlt = values => ({$not: {$lt: moment(values[0]).toDate()}});

specs.between = values => ({$gte: moment(values[0]).toDate(), $lt: moment(values[1]).toDate()});
specs.notBetween = values => ({$not: {$gte: moment(values[0]).toDate(), $lt: moment(values[1]).toDate()}});

specs.in = (values, name) => {
  let dates = _.split(values[0], /[,;]/);
  dates = _.map(dates, date => ({[name]: {$gte: moment(date).toDate(), $lt: moment(date).add(1, 'days').toDate()}}));
  return {$or: dates};
};

specs.nin = (values, name) => {
  let dates = _.split(values[0], /[,;]/);
  dates = _.map(dates, date => ({[name]: {$gte: moment(date).toDate(), $lt: moment(date).add(1, 'days').toDate()}}));
  return {$nor: dates};
};
