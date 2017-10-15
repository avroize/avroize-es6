# avroize-es6

[![Build Status](https://travis-ci.org/avroize/avroize-es6.svg?branch=master)](https://travis-ci.org/avroize/avroize-es6) 
[![Coverage Status](https://coveralls.io/repos/github/avroize/avroize-es6/badge.svg)](https://coveralls.io/github/avroize/avroize-es6) 
[![npm version](https://img.shields.io/npm/v/avroize-es6.svg?style=flat)](https://www.npmjs.com/package/avroize-es6)

Transform your data into valid Avro JSON for any given Avro schema.

### Support for MVP release:
* Primitive types: *Boolean, Double, Float, Integer, Long, String*
* Complex types: *Arrays, Records, Unions (null only)*
* JSON Avroizer

## Getting Started

Install Avroize using `npm`:

```
npm install --save avroize-es6
```

Avroize JSON data:

```javascript
import {getJSONAvroizer} from 'avroize-es6'

const avroSchema = {
    "name": "example",
    "namespace": "avro.test",
    "type": "record",
    "fields":[{
        "default": "",
        "name": "field1",
        "type": "string"
    },{
        "default": null,
        "name": "field2",
        "type": ["null", "string"]
    }]
};

const avroizer = getJSONAvroizer(avroSchema);

const result = avroizer.avroize({"field1":"test"});

console.log(result); // {"field1": "test", "field2": null}
```