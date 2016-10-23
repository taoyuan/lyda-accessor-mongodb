"use strict";

const _ = require('lodash');
const fd = require('../lib/filter/date');

_.forEach(fd.micros, (micro, name) => {
  console.log(name + ':', micro());
});
