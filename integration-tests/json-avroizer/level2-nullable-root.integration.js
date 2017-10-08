
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

const level2NullableRootAvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1.1,
        "name": "field1",
        "type": avroTypes.DOUBLE
    },{
        "default": null,
        "name": "level2",
        "type": [avroTypes.NULL, {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": "2",
                "name": "field2",
                "type": avroTypes.STRING
            },{
                "default": null,
                "name": "level3a",
                "type": [avroTypes.NULL, {
                    "type": avroTypes.RECORD,
                    "name": "level3a_data",
                    "fields": [{
                        "default": 0,
                        "name": "field3",
                        "type": avroTypes.INTEGER
                    }]
                }]
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
       describe("two level root nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: undefined
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": undefined
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value deep 1 level", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: undefined
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value deep 1 level", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: {
                               "avro.test.level3b_data": undefined
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {}
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: null,
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value deep 1 level", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: {
                               "avro.test.level3b_data": {}
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               "avro.test.level3a_data": {
                                   field3: 1
                               }
                           },
                           level3b: {
                               "avro.test.level3b_data": {
                                   field4: null
                               }
                           }
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("detached data node", () => {
               avroizer = new JSONAvroizer(level2NullableRootAvroSchema);

               data = {
                   level3: {
                       "avro.test.level3_data": {
                           field3: 1
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2NullableRootAvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });
   });
});