
import {isDefined, isInteger, isObject, isString, getDefaultValueForAvroType} from "../src/utilities";
import {avroTypes} from "../src/constants/avro-types";

let result;

describe("utilities", () => {
    describe("isDefined", () => {
        test("return false if value is undefined", () => {
            result = isDefined(undefined);

            expect(result).toBeFalsy();
        });

        test("return false if value is null", () => {
            result = isDefined(null);

            expect(result).toBeFalsy();
        });

        test("return true if value is otherwise", () => {
            result = isDefined("value");

            expect(result).toBeTruthy();
        });
    });

    describe("isInteger", () => {
        test("return true if value is integer", () => {
            result = isInteger(3);

            expect(result).toBeTruthy();
        });

        test("return false if value is float", () => {
            result = isInteger(3.1);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = isInteger(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isObject", () => {
        test("return true if value is object", () => {
            result = isObject({});

            expect(result).toBeTruthy();
        });

        test("return false if value is null", () => {
            result = isObject(null);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = isObject(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isString", () => {
        test("return true if value is string", () => {
            result = isString("");

            expect(result).toBeTruthy();
        });

        test("return false if value is otherwise", () => {
            result = isString(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("getDefaultValueForAvroType", () => {
        test("string avro type returns empty string", () => {
            result = getDefaultValueForAvroType(avroTypes.STRING);

            expect(result).toEqual("");
        });

        test("integer avro type returns zero", () => {
            result = getDefaultValueForAvroType(avroTypes.INTEGER);

            expect(result).toEqual(0);
        });

        test("default returns undefined", () => {
            result = getDefaultValueForAvroType(null);

            expect(result).toBeUndefined();
        });
    });
});