
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

const level2ArrayAvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1.1,
        "name": "field1",
        "type": avroTypes.DOUBLE
    },{
        "default": null,
        "name": "field2",
        "type": ["null", avroTypes.LONG]
    },{
        "name": "level2",
        "type": {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": null,
                "name": "field3",
                "type": [avroTypes.NULL, avroTypes.FLOAT]
            },{
                "default": "4",
                "name": "field4",
                "type": avroTypes.STRING
            },{
                "default": [],
                "name": "level2a",
                "type": {
                    "default": [],
                    "type": avroTypes.ARRAY,
                    "items": {
                        "name": "level2a_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": 5,
                            "name": "field5",
                            "type": avroTypes.INTEGER
                        },{
                            "default": null,
                            "name": "field6",
                            "type": [avroTypes.NULL, avroTypes.FLOAT]
                        },{
                            "name": "level3a",
                            "type": {
                                "name": "level3a_data",
                                "type": avroTypes.RECORD,
                                "fields":[{
                                    "default": "7",
                                    "name": "field7",
                                    "type": avroTypes.STRING
                                }]
                            }
                        }]
                    }
                }
            },{
                "default": null,
                "name": "level2b",
                "type": ["null", {
                    "default": [],
                    "type": avroTypes.ARRAY,
                    "items": {
                        "name": "level2b_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": 8,
                            "name": "field8",
                            "type": avroTypes.INTEGER
                        },{
                            "default": null,
                            "name": "field9",
                            "type": [avroTypes.NULL, avroTypes.FLOAT]
                        },{
                            "name": "level3b",
                            "type": ["null", {
                                "name": "level3b_data",
                                "type": avroTypes.RECORD,
                                "fields":[{
                                    "default": "10",
                                    "name": "field10",
                                    "type": avroTypes.STRING
                                }]
                            }]
                        }]
                    }
                }]
            }]
        }
    }]
};

let avroizer, data, expected, isValid, result;

function isAvroValid(schema, avroizeData) {
    const avroType = avsc.parse(schema);
    return avroType.isValid(avroizeData);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
       describe("two level array", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level2ArrayAvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level2ArrayAvroSchema, result);

               expected = {
                   field1: 0,
                   field2: null,
                   level2: {
                       field3: null,
                       field4: "",
                       level2a: [],
                       level2b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level2ArrayAvroSchema);
               data = {
                   field2: { long: 10000 },
                   level2: {
                       field3: { float: 1.1 },
                       level2a: [null, undefined, {
                           field5: 5
                       }, {}, {
                           field6: { float: 2.2 },
                           level3a: {
                               field7: "7"
                           }
                       }]
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2ArrayAvroSchema, result);

               expected = {
                   field1: 0,
                   field2: { long: 10000 },
                   level2: {
                       field3: { float: 1.1 },
                       field4: "",
                       level2a: [{
                           field5: 5,
                           field6: null,
                           level3a: {
                               field7: ""
                           }
                       }, {
                           field5: 0,
                           field6: null,
                           level3a: {
                               field7: ""
                           }
                       }, {
                           field5: 0,
                           field6: { float: 2.2 },
                           level3a: {
                               field7: "7"
                           }
                       }],
                       level2b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values nullable", () => {
               avroizer = new JSONAvroizer(level2ArrayAvroSchema);
               data = {
                   field1: 1,
                   level2: {
                       field4: "4",
                       level2b: {
                           "array": [null, undefined, {
                               field8: 8,
                               level3b: null
                           }, {}, {
                               field9: { float: 2.2 },
                               level3b: {
                                   "avro.test.level3b_data": {
                                       field10: "10"
                                   }
                               }
                           }]
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2ArrayAvroSchema, result);

               expected = {
                   field1: 1,
                   field2: null,
                   level2: {
                       field3: null,
                       field4: "4",
                       level2a: [],
                       level2b: {
                           "array": [{
                               field8: 8,
                               field9: null,
                               level3b: null
                           }, {
                               field8: 0,
                               field9: null,
                               level3b: null
                           }, {
                               field8: 0,
                               field9: { float: 2.2 },
                               level3b: {
                                   "avro.test.level3b_data": {
                                       field10: "10"
                                   }
                               }
                           }]
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });
   });
});