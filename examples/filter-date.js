"use strict";

const fd = require('../lib/filter/date');

console.log('eq:', fd('test', 'eq', '#today'));
console.log('in:', fd('test', 'in', '2016-01-01;2016-01-02,2016-02-02'));
