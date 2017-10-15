import {INVALID_SCHEMA, getJSONAvroizer} from "../src/index";
import JSONAvroizer from "../src/avroizer/json-avroizer";

let result;

describe("index", () => {
    describe("getJSONAvroizer", () => {
        test("throw if not valid avro schema", () => {
            expect(() => getJSONAvroizer(undefined)).toThrow(INVALID_SCHEMA);
        });

        test("return JSON avroizer", () => {
            result = getJSONAvroizer({});

            expect(result).toBeInstanceOf(JSONAvroizer);
        });
    });
});