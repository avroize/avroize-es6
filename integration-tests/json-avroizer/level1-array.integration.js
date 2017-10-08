
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

const level1ArrayAvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": true,
        "name": "field1",
        "type": avroTypes.BOOLEAN
    },{
        "default": null,
        "name": "field2",
        "type": [avroTypes.NULL, avroTypes.DOUBLE]
    },{
        "default": [],
        "name": "level1a",
        "type": {
            "default": [],
            "type": avroTypes.ARRAY,
            "items": {
                "name": "level1a_data",
                "type": avroTypes.RECORD,
                "fields":[{
                    "default": 1,
                    "name": "field3",
                    "type": avroTypes.INTEGER
                },{
                    "default": null,
                    "name": "field4",
                    "type": [avroTypes.NULL, avroTypes.FLOAT]
                },{
                    "name": "level2a",
                    "type": {
                        "name": "level2a_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": "4",
                            "name": "field5",
                            "type": avroTypes.STRING
                        }]
                    }
                }]
            }
        }
    },{
        "default": null,
        "name": "level1b",
        "type": ["null", {
            "default": [],
            "type": avroTypes.ARRAY,
            "items": {
                "name": "level1b_data",
                "type": avroTypes.RECORD,
                "fields":[{
                    "default": 1,
                    "name": "field6",
                    "type": avroTypes.INTEGER
                },{
                    "default": null,
                    "name": "field7",
                    "type": [avroTypes.NULL, avroTypes.FLOAT]
                },{
                    "name": "level2b",
                    "type": {
                        "name": "level2b_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": "8",
                            "name": "field8",
                            "type": avroTypes.STRING
                        }]
                    }
                }]
            }
        }]
    }]
};

let avroizer, data, expected, isValid, result;

function isAvroValid(schema, avroizeData) {
    const avroType = avsc.parse(schema);
    return avroType.isValid(avroizeData);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
       describe("one level array", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level1ArrayAvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level1ArrayAvroSchema, result);

               expected = {
                   field1: false,
                   field2: null,
                   level1a: [],
                   level1b: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level1ArrayAvroSchema);
               data = {
                   field2: { double: 1.1 },
                   level1a: [null, undefined, {
                       field3: 1,
                       level2a: {
                           field5: "1"
                       }
                   }, {}, {
                       field3: 2,
                       field4: { float: 1.1 }
                   }]
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1ArrayAvroSchema, result);

               expected = {
                   field1: false,
                   field2: { double: 1.1 },
                   level1a: [{
                       field3: 1,
                       field4: null,
                       level2a: {
                           field5: "1"
                       }
                   },{
                       field3: 0,
                       field4: null,
                       level2a: {
                           field5: ""
                       }
                   },{
                       field3: 2,
                       field4: { float: 1.1 },
                       level2a: {
                           field5: ""
                       }
                   }],
                   level1b: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values nullable", () => {
               avroizer = new JSONAvroizer(level1ArrayAvroSchema);
               data = {
                   field1: true,
                   level1b: {
                       "array": [null, undefined, {
                           field6: 1,
                           level2b: {
                               field8: "1"
                           }
                       }, {}, {
                           field7: { float: 1.1 }
                       }]
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1ArrayAvroSchema, result);

               expected = {
                   field1: true,
                   field2: null,
                   level1a: [],
                   level1b: {
                       "array": [{
                           field6: 1,
                           field7: null,
                           level2b: {
                               field8: "1"
                           }
                       },{
                           field6: 0,
                           field7: null,
                           level2b: {
                               field8: ""
                           }
                       },{
                           field6: 0,
                           field7: { float: 1.1 },
                           level2b: {
                               field8: ""
                           }
                       }]
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });
   });
});