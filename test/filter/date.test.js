'use strict';

var assert = require('chai').assert;
var DateFilter = require('../../lib/filter/date');

describe('Filter/date', function () {
  it('should resolve today', function () {
    console.log(DateFilter.micros.TODAY());
  });
});
