
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import avroTypes from "../../src/constants/avro-types";
import avsc from "avsc";

const rootAvroSchema = {
    "default": "1",
    "name": "field1",
    "type": avroTypes.STRING
};

let avroizer, isValid, result;

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
   });
});