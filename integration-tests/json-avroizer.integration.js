
import JSONAvroizer from "../src/avroizer/json-avroizer";
import {avroTypes} from "../src/constants/avro-types";
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
        "default": "1",
        "name": "field1",
        "type": avroTypes.STRING
    },{
        "default": 2,
        "name": "field2",
        "type": avroTypes.INTEGER
    }]
};

const level2AvroSchema = {
    "name": "level1",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1,
        "name": "field1",
        "type": avroTypes.INTEGER
    },{
        "name": "level2",
        "type": {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": 2,
                "name": "field2",
                "type": avroTypes.INTEGER
            },{
                "default": "3",
                "name": "field3",
                "type": avroTypes.STRING
            },{
                "default": "4",
                "name": "field4",
                "type": avroTypes.STRING
            }]
        }
    }]
};

const level3AvroSchema = {
    "name": "level1",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1,
        "name": "field1",
        "type": avroTypes.INTEGER
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
                "default": 3,
                "name": "field3",
                "type": avroTypes.INTEGER
            },{
                "name": "level3",
                "type": {
                    "name": "level3_data",
                    "type": avroTypes.RECORD,
                    "fields":[{
                        "default": 4,
                        "name": "field4",
                        "type": avroTypes.INTEGER
                    },{
                        "default": "5",
                        "name": "field5",
                        "type": avroTypes.STRING
                    }]
                }
            }]
        }
    }]
};

let avroValue, avroizer, data, expected, result;

function getAvroValue(schema, avroizeData) {
    const avroType = avsc.Type.forSchema(schema);
    const avroBuffer = avroType.toBuffer(avroizeData);
    return avroType.fromBuffer(avroBuffer);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
       test("root", () => {
           avroizer = new JSONAvroizer(rootAvroSchema);

           result = avroizer.avroize("2");
           avroValue = getAvroValue(rootAvroSchema, result);

           expect(result).toEqual("2");
           expect(avroValue).toBeDefined();
       });

       test("one level", () => {
           avroizer = new JSONAvroizer(level1AvroSchema);
           data = {
               field1: "1"
           };

           result = avroizer.avroize(data);
           avroValue = getAvroValue(level1AvroSchema, result);

           expected = {
               field1: "1",
               field2: 0
           };
           expect(result).toEqual(expected);
           expect(avroValue).toBeDefined();
       });

       test("two levels", () => {
           avroizer = new JSONAvroizer(level2AvroSchema);
           data = {
               field1: 1,
               level2: {
                   field3: "1"
               }
           };

           result = avroizer.avroize(data);
           avroValue = getAvroValue(level2AvroSchema, result);

           expected = {
               field1: 1,
               level2: {
                   field2: 0,
                   field3: "1",
                   field4: ""
               }
           };
           expect(result).toEqual(expected);
           expect(avroValue).toBeDefined();
       });

       test("three levels", () => {
           avroizer = new JSONAvroizer(level3AvroSchema);
           data = {
               field1: 1,
               level2: {
                   field2: "1",
                   level3: {
                       field5: "1"
                   }
               }
           };

           result = avroizer.avroize(data);
           avroValue = getAvroValue(level3AvroSchema, result);

           expected = {
               field1: 1,
               level2: {
                   field2: "1",
                   field3: 0,
                   level3: {
                       field4: 0,
                       field5: "1"
                   }
               }
           };
           expect(result).toEqual(expected);
           expect(avroValue).toBeDefined();
       });
   });
});