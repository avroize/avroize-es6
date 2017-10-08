
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

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

let avroizer, data, expected, isValid, result;

function isAvroValid(schema, avroizeData) {
    const avroType = avsc.parse(schema);
    return avroType.isValid(avroizeData);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
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
   });
});