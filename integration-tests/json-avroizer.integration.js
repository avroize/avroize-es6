
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
    }]
};

const level2AvroSchema = {
    "name": "level1",
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
            }]
        }
    }]
};

const level3AvroSchema = {
    "name": "level1",
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
                    }]
                }
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
       test("root", () => {
           avroizer = new JSONAvroizer(rootAvroSchema);

           result = avroizer.avroize("2");
           isValid = isAvroValid(rootAvroSchema, result);

           expect(result).toEqual("2");
           expect(isValid).toBeTruthy();
       });

       test("one level", () => {
           avroizer = new JSONAvroizer(level1AvroSchema);
           data = {
               field1: { string: "1" },
               field3: true,
               field4: null,
               field6: 1.1
           };

           result = avroizer.avroize(data);
           isValid = isAvroValid(level1AvroSchema, result);

           expected = {
               field1: { string: "1" },
               field2: 0,
               field3: true,
               field4: null,
               field5: 0,
               field6: 1.1
           };
           expect(result).toEqual(expected);
           expect(isValid).toBeTruthy();
       });

       test("two levels", () => {
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
                   field6: { int: 1 }
               }
           };
           expect(result).toEqual(expected);
           expect(isValid).toBeTruthy();
       });

       test("three levels", () => {
           avroizer = new JSONAvroizer(level3AvroSchema);
           data = {
               field1: 1.1,
               field2: { boolean: false },
               level2: {
                   field4: 10000,
                   level3: {
                       field6: 1.1
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
                       field6: 1.1
                   }
               }
           };
           expect(result).toEqual(expected);
           expect(isValid).toBeTruthy();
       });
   });
});