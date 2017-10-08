
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

const level1NullableAvroSchema = {
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

let avroizer, data, expected, isValid, result;

function isAvroValid(schema, avroizeData) {
    const avroType = avsc.parse(schema);
    return avroType.isValid(avroizeData);
}

describe("JSONAvroizer", () => {
   describe("avroize", () => {
       describe("one level nullable", () => {
           test("empty", () => {
               avroizer = new JSONAvroizer(level1NullableAvroSchema);

               result = avroizer.avroize();
               isValid = isAvroValid(level1NullableAvroSchema, result);

               expected = {
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("values", () => {
               avroizer = new JSONAvroizer(level1NullableAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {
                           field1: "1"
                       }
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1NullableAvroSchema, result);

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
               avroizer = new JSONAvroizer(level1NullableAvroSchema);

               data = {
                   level2: undefined
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1NullableAvroSchema, result);

               expected = {
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("bad data node value", () => {
               avroizer = new JSONAvroizer(level1NullableAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": undefined
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1NullableAvroSchema, result);

               expected = {
                   level2: null
               };
               expect(result).toEqual(expected);
               expect(isValid).toBeTruthy();
           });

           test("empty data node value", () => {
               avroizer = new JSONAvroizer(level1NullableAvroSchema);

               data = {
                   level2: {
                       "avro.test.level2_data": {}
                   }
               };

               result = avroizer.avroize(data);
               isValid = isAvroValid(level1NullableAvroSchema, result);

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
   });
});