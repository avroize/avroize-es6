
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

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

let avroizer, data, expected, isValid, result;

function isAvroValid(schema, avroizeData) {
    const avroType = avsc.parse(schema);
    return avroType.isValid(avroizeData);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
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
   });
});