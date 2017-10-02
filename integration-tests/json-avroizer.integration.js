
import JSONAvroizer from "../src/avroizer/json-avroizer";
import avroTypes from "../src/constants/avro-types";
import avsc from "avsc";

const rootAvroSchema = {
    "default": "1",
    "name": "field1",
    "type": avroTypes.STRING
};

const level1AvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": null,
        "name": "field1",
        "type": [avroTypes.NULL, avroTypes.STRING]
    },{
        "default": 2,
        "name": "field2",
        "type": avroTypes.INTEGER
    },{
        "default": true,
        "name": "field3",
        "type": avroTypes.BOOLEAN
    },{
        "default": null,
        "name": "field4",
        "type": [avroTypes.NULL, avroTypes.DOUBLE]
    },{
        "default": 10000,
        "name": "field5",
        "type": avroTypes.LONG
    },{
        "default": 1.1,
        "name": "field6",
        "type": avroTypes.FLOAT
    },{
        "default": null,
        "name": "field7",
        "type": [avroTypes.NULL, {
            "type": avroTypes.ARRAY,
            "items": avroTypes.DOUBLE
        }]
    }]
};

const nullableLevel1AvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": null,
        "name": "level2",
        "type": [avroTypes.NULL, {
            "type": avroTypes.RECORD,
            "name": "level2_data",
            "fields": [{
                "default": "1",
                "name": "field1",
                "type": avroTypes.STRING
            }]
        }]
    }]
};

const level2AvroSchema = {
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
                "default": null,
                "name": "field5",
                "type": [avroTypes.NULL, avroTypes.BOOLEAN]
            },{
                "default": null,
                "name": "field6",
                "type": [avroTypes.NULL, avroTypes.INTEGER]
            },{
                "default": [],
                "name": "field7",
                "type": {
                    "type": avroTypes.ARRAY,
                    "items": avroTypes.FLOAT
                }
            }]
        }
    }]
};

const nullableLevel2AvroSchema = {
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
        }
    }]
};

const nullableRootLevel2AvroSchema = {
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

const level3AvroSchema = {
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
        "type": [avroTypes.NULL, avroTypes.BOOLEAN]
    },{
        "name": "level2",
        "type": {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": null,
                "name": "field3",
                "type": [avroTypes.NULL, avroTypes.STRING]
            },{
                "default": 10000,
                "name": "field4",
                "type": avroTypes.LONG
            },{
                "name": "level3",
                "type": {
                    "name": "level3_data",
                    "type": avroTypes.RECORD,
                    "fields":[{
                        "default": null,
                        "name": "field5",
                        "type": [avroTypes.NULL, avroTypes.FLOAT]
                    },{
                        "default": 1.1,
                        "name": "field6",
                        "type": avroTypes.FLOAT
                    },{
                        "default": [],
                        "name": "field7",
                        "type": {
                            "type": avroTypes.ARRAY,
                            "items": avroTypes.BOOLEAN
                        }
                    }]
                }
            }]
        }
    }]
};

const nullableLevel3AvroSchema = {
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

const nullableRootLevel3AvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1.1,
        "name": "field1",
        "type": avroTypes.DOUBLE
    },{
        "name": "level2",
        "type": [avroTypes.NULL, {
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
                        "name": "level4a",
                        "type": [avroTypes.NULL, {
                            "type": avroTypes.RECORD,
                            "name": "level4a_data",
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
                    },{
                        "name": "level4b",
                        "type": {
                            "type": avroTypes.RECORD,
                            "name": "level4b_data",
                            "fields": [{
                                "default": null,
                                "name": "field7",
                                "type": [avroTypes.NULL, avroTypes.DOUBLE]
                            },{
                                "default": 0,
                                "name": "field8",
                                "type": avroTypes.FLOAT
                            }]
                        }
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
       describe("root", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(rootAvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(rootAvroSchema, result);

               expect(result).toEqual("");
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(rootAvroSchema);

               result = avroizer.avroize("2");
               isValid = isAvroValid(rootAvroSchema, result);

               expect(result).toEqual("2");
               expect(isValid).toBeTruthy();
           });
       });

       describe("one level", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level1AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level1AvroSchema, result);

               expected = {
                   field1: null,
                   field2: 0,
                   field3: false,
                   field4: null,
                   field5: 0,
                   field6: 0,
                   field7: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level1AvroSchema);
               data = {
                   field1: { string: "1" },
                   field3: true,
                   field4: null,
                   field6: 1.1,
                   field7: { array: [1.1] }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1AvroSchema, result);

               expected = {
                   field1: { string: "1" },
                   field2: 0,
                   field3: true,
                   field4: null,
                   field5: 0,
                   field6: 1.1,
                   field7: { array: [1.1] }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });

       describe("one level nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(nullableLevel1AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(nullableLevel1AvroSchema, result);

               expected = {
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(nullableLevel1AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           field1: "1"
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel1AvroSchema, result);

               expected = {
                   level2: {
                       "avro.test.level2_data": {
                           field1: "1"
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value", () => {
               avroizer = new JSONAvroizer(nullableLevel1AvroSchema);

               data = {
                   level2: undefined
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel1AvroSchema, result);

               expected = {
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(nullableLevel1AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": undefined
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel1AvroSchema, result);

               expected = {
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value", () => {
               avroizer = new JSONAvroizer(nullableLevel1AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {}
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel1AvroSchema, result);

               expected = {
                   level2: {
                       "avro.test.level2_data": {
                           field1: ""
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });

       describe("two levels", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level2AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level2AvroSchema, result);

               expected = {
                   field1: 0,
                   field2: null,
                   level2: {
                       field3: null,
                       field4: "",
                       field5: null,
                       field6: null,
                       field7: []
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level2AvroSchema);
               data = {
                   field2: { long: 10000 },
                   level2: {
                       field3: { float: 1.1 },
                       field5: null,
                       field6: { int: 1 }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level2AvroSchema, result);

               expected = {
                   field1: 0,
                   field2: { long: 10000 },
                   level2: {
                       field3: { float: 1.1 },
                       field4: "",
                       field5: null,
                       field6: { int: 1 },
                       field7: []
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });

       describe("two level nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(nullableLevel2AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(nullableLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: null,
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(nullableLevel2AvroSchema);

               data = {
                   level2: {
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value", () => {
               avroizer = new JSONAvroizer(nullableLevel2AvroSchema);

               data = {
                   level2: {
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       },
                       level3b: undefined
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(nullableLevel2AvroSchema);

               data = {
                   level2: {
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       },
                       level3b: {
                           "avro.test.level3b_data": undefined
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       },
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value", () => {
               avroizer = new JSONAvroizer(nullableLevel2AvroSchema);

               data = {
                   level2: {
                       level3a: {
                           "avro.test.level3a_data": {
                               field3: 1
                           }
                       },
                       level3b: {
                           "avro.test.level3b_data": {}
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
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
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("detached data node", () => {
               avroizer = new JSONAvroizer(nullableLevel2AvroSchema);

               data = {
                   level3: {
                       "avro.test.level3_data": {
                           field3: 1
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       field2: "",
                       level3a: null,
                       level3b: null
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });

       describe("two level root nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

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
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

               data = {
                   level2: undefined
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": undefined
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value deep 1 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

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
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

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
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {}
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

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
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableRootLevel2AvroSchema);

               data = {
                   level3: {
                       "avro.test.level3_data": {
                           field3: 1
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel2AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });

       describe("three levels", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level3AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level3AvroSchema, result);

               expected = {
                   field1: 0,
                   field2: null,
                   level2: {
                       field3: null,
                       field4: 0,
                       level3: {
                           field5: null,
                           field6: 0,
                           field7: []
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level3AvroSchema);
               data = {
                   field1: 1.1,
                   field2: { boolean: false },
                   level2: {
                       field4: 10000,
                       level3: {
                           field6: 1.1,
                           field7: [true, false]
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level3AvroSchema, result);

               expected = {
                   field1: 1.1,
                   field2: { boolean: false },
                   level2: {
                       field3: null,
                       field4: 10000,
                       level3: {
                           field5: null,
                           field6: 1.1,
                           field7: [true, false]
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });
       });

       describe("three level nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(nullableLevel3AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(nullableLevel3AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableLevel3AvroSchema);

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
               isValid = isAvroValid(nullableLevel3AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableLevel3AvroSchema);

               data = {
                   level2: {
                       level3a: {
                           level4: undefined
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel3AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableLevel3AvroSchema);

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
               isValid = isAvroValid(nullableLevel3AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableLevel3AvroSchema);

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
               isValid = isAvroValid(nullableLevel3AvroSchema, result);

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
               avroizer = new JSONAvroizer(nullableLevel3AvroSchema);

               data = {
                   level4: {
                       "avro.test.level4_data": {
                           field5: 1
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableLevel3AvroSchema, result);

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

       describe("three level root nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               level4a: null
                           },
                           level3b: {
                               "avro.test.level3b_data": {
                                   field4: { boolean: true },
                                   level4b: {
                                       field7: { double: 1 }
                                   }
                               }
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 0,
                               level4a: null
                           },
                           level3b: {
                               "avro.test.level3b_data": {
                                   field4: { boolean: true },
                                   level4b: {
                                       field7: { double: 1 },
                                       field8: 0
                                   }
                               }
                           }
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: undefined
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": undefined
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value deep 1 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               field3: 1
                           },
                           level3b: undefined
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 1,
                               level4a: null
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad node value deep 2 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               field3: 1,
                               level4a: undefined
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 1,
                               level4a: null
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value deep 1 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               field3: 1
                           },
                           level3b: {
                               "avro.test.level3b_data": undefined
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 1,
                               level4a: null
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value deep 2 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               field3: 1,
                               level4a: {
                                   "avro.test.level4a_data": undefined
                               }
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 1,
                               level4a: null
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {}
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 0,
                               level4a: null
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value deep 1 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               field3: 1
                           },
                           level3b: {
                               "avro.test.level3b_data": {}
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 1,
                               level4a: null
                           },
                           level3b: {
                               "avro.test.level3b_data": {
                                   field4: null,
                                   level4b: {
                                       field7: null,
                                       field8: 0
                                   }
                               }
                           }
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value deep 2 level", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           level3a: {
                               field3: 1,
                               level4a: {
                                   "avro.test.level4a_data": {}
                               }
                           }
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

               expected = {
                   field1: 0,
                   level2: {
                       "avro.test.level2_data": {
                           field2: "",
                           level3a: {
                               field3: 1,
                               level4a: {
                                   "avro.test.level4a_data": {
                                       field5: null,
                                       field6: 0
                                   }
                               }
                           },
                           level3b: null
                       }
                   }
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("detached data node", () => {
               avroizer = new JSONAvroizer(nullableRootLevel3AvroSchema);

               data = {
                   level3: {
                       "avro.test.level3_data": {
                           field3: 1
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(nullableRootLevel3AvroSchema, result);

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