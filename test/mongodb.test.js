"use strict";

const assert = require('chai').assert;
const MongoDBAccessor = require('..');

describe('mongodb', function () {

  describe('#buildAggregation', () => {

    it('should build aggregation', function () {
      const accessor = new MongoDBAccessor();
      const aggregation = accessor.buildAggregation({
        fields: [{
          name: 'date',
          columnName: 'date',
          type: 'date',
          aggregation: 'year'
        }, {
          name: 'date',
          columnName: 'date',
          type: 'date',
          aggregation: 'month'
        }, {
          name: 'date',
          columnName: 'date',
          type: 'date',
          aggregation: 'dayOfMonth'
        }, {
          name: 'price',
          type: 'number',
          expression: 'price * quantity',
          aggregation: 'sum'
        }, {
          name: 'quantity',
          type: 'number',
          columnName: 'quantity',
          aggregation: 'avg'
        }, {
          name: 'item',
          type: 'number',
          columnName: 'item',
          aggregation: 'count'
        }],
        orders: [{
          name: "price",
          direction: 1
        }],
      });
      console.log(JSON.stringify(aggregation));
      assert.deepEqual(aggregation, [
        {
          "$project": {
            "date_year": {
              "$year": "$date"
            },
            "date": "$date",
            "date_month": {
              "$month": "$date"
            },
            "date_dayOfMonth": {
              "$dayOfMonth": "$date"
            },
            "price": {
              "$multiply": [
                "$price",
                "$quantity"
              ]
            },
            "quantity": "$quantity",
            "item": "$item"
          }
        },
        {
          "$group": {
            "price_sum": {
              "$sum": "$price"
            },
            "quantity_avg": {
              "$avg": "$quantity"
            },
            "item_count": {
              "$sum": 1
            },
            "_id": {
              "date_year": "$date_year",
              "date_month": "$date_month",
              "date_dayOfMonth": "$date_dayOfMonth"
            }
          }
        },
        {
          "$sort": {
            "price_sum": 1
          }
        },
        {
          "$limit": 50
        }
      ])
    });
  });
});
