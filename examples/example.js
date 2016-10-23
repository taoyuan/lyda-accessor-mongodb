"use strict";

const MongoDB = require('../');

const accessor = new MongoDB({database: 'formioapp'});

// accessor.discoverResources()
//   .then(resources => {
//     console.log(resources);
//   })
//   .finally(() => accessor.disconnect());

accessor.discoverSchemas('forms')
  .then(schema => {
    console.log(schema);
  })
  .finally(() => accessor.disconnect());
