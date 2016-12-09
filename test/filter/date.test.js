'use strict';

const assert = require('chai').assert;
const DateFilter = require('../../lib/filter/date');

describe('Filter/date', function () {
  it('should resolve today', function () {
    assert.lengthOf(DateFilter.micros.today(), 2);
  });
});
