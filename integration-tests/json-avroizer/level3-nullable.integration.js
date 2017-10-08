
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

const level3NullableAvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1.1,
        "name": "field1",
        "type": avroTypes.DOUBLE
    },{
        "name": "level2",
        "type": {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": "2",
                "name": "field2",
                "type": avroTypes.STRING
            },{
                "name": "level3a",
                "type": {
                    "type": avroTypes.RECORD,
                    "name": "level3a_data",
                    "fields": [{
                        "default": 0,
                        "name": "field3",
                        "type": avroTypes.LONG
                    },{
                        "default": null,
                        "name": "level4",
                        "type": [avroTypes.NULL, {
                            "type": avroTypes.RECORD,
                            "name": "level4_data",
                            "fields": [{
                                "default": null,
                                "name": "field5",
                                "type": [avroTypes.NULL, avroTypes.DOUBLE]
                            },{
                                "default": 0,
                                "name": "field6",
                                "type": avroTypes.FLOAT
                            }]
                        }]
                    }]
                }
            },{
                "default": null,
                "name": "level3b",
                "type": [avroTypes.NULL, {
                    "type": avroTypes.RECORD,
                    "name": "level3b_data",
                    "fields": [{
                        "default": null,
                        "name": "field4",
                        "type": [avroTypes.NULL, avroTypes.BOOLEAN]
                    }]
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
       describe("three level nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level3NullableAvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level3NullableAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           field3: 0,
                           level4: null
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level3NullableAvroSchema);

               data = {
                   level2: {
                       field2: "1",
                       level3a: {
                           level4: {
                               "avro.test.level4_data": {
                                    field6: 1
                               }
                           }
                       },
                       level3b: {
                           "avro.test.level3b_data": {
                               field4: { boolean: true }
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level3NullableAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "1",
                       level3a: {
                           field3: 0,
                           level4: {
                               "avro.test.level4_data": {
                                   field5: null,
                                   field6: 1
                               }
                           }
                       },
                       level3b: {
                           "avro.test.level3b_data": {
                               field4: { boolean: true }
                           }
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value", () => {
               avroizer = new JSONAvroizer(level3NullableAvroSchema);

               data = {
                   level2: {
                       level3a: {
                           level4: undefined
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level3NullableAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           field3: 0,
                           level4: null
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(level3NullableAvroSchema);

               data = {
                   level2: {
                       level3a: {
                           level4: {
                               "avro.test.level4_data": undefined
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level3NullableAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           field3: 0,
                           level4: null
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value", () => {
               avroizer = new JSONAvroizer(level3NullableAvroSchema);

               data = {
                   level2: {
                       level3a: {
                           level4: {
                               "avro.test.level4_data": {}
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level3NullableAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           field3: 0,
                           level4: {
                               "avro.test.level4_data": {
                                   field5: null,
                                   field6: 0
                               }
                           }
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("detached data node", () => {
               avroizer = new JSONAvroizer(level3NullableAvroSchema);

               data = {
                   level4: {
                       "avro.test.level4_data": {
                           field5: 1
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level3NullableAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           field3: 0,
                           level4: null
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });
   });
});