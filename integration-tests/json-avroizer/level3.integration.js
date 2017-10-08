
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

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

let avroizer, data, expected, isValid, result;

function isAvroValid(schema, avroizeData) {
    const avroType = avsc.parse(schema);
    return avroType.isValid(avroizeData);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
       describe("three level", () => {
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
   });
});