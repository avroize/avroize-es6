
import * as utilities from "../src/utilities";
import avroTypes from "../src/constants/avro-types";

let result;

describe("utilities", () => {
    describe("isDefined", () => {
        test("return false if value is undefined", () => {
            result = utilities.isDefined(undefined);

            expect(result).toBeFalsy();
        });

        test("return false if value is null", () => {
            result = utilities.isDefined(null);

            expect(result).toBeFalsy();
        });

        test("return true if value is otherwise", () => {
            result = utilities.isDefined("value");

            expect(result).toBeTruthy();
        });
    });

    describe("isArray", () => {
        test("return true if value is an array", () => {
            result = utilities.isArray([]);

            expect(result).toBeTruthy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isArray(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isBoolean", () => {
        test("return true if value is boolean", () => {
            result = utilities.isBoolean(false);

            expect(result).toBeTruthy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isBoolean(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isDouble", () => {
        test("return true if value is float", () => {
            result = utilities.isDouble(3.1);

            expect(result).toBeTruthy();
        });

        test("return false if value is integer", () => {
            result = utilities.isDouble(3);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isDouble(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isFloat", () => {
        test("return true if value is float", () => {
            result = utilities.isFloat(3.1);

            expect(result).toBeTruthy();
        });

        test("return false if value is integer", () => {
            result = utilities.isFloat(3);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isFloat(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isInteger", () => {
        test("return true if value is integer", () => {
            result = utilities.isInteger(3);

            expect(result).toBeTruthy();
        });

        test("return false if value is float", () => {
            result = utilities.isInteger(3.1);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isInteger(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isLong", () => {
        test("return true if value is integer", () => {
            result = utilities.isLong(3);

            expect(result).toBeTruthy();
        });

        test("return false if value is float", () => {
            result = utilities.isLong(3.1);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isLong(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isObject", () => {
        test("return true if value is object", () => {
            result = utilities.isObject({});

            expect(result).toBeTruthy();
        });

        test("return false if value is null", () => {
            result = utilities.isObject(null);

            expect(result).toBeFalsy();
        });

        test("return false if value is array", () => {
            result = utilities.isObject([]);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isObject(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isString", () => {
        test("return true if value is string", () => {
            result = utilities.isString("");

            expect(result).toBeTruthy();
        });

        test("return false if value is otherwise", () => {
            result = utilities.isString(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("getDefaultValueForAvroType", () => {
        test("boolean avro type returns false", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.BOOLEAN);

            expect(result).toBeFalsy();
        });

        test("integer avro type returns zero", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.INTEGER);

            expect(result).toEqual(0);
        });

        test("string avro type returns empty string", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.STRING);

            expect(result).toEqual("");
        });

        test("default returns undefined", () => {
            result = utilities.getDefaultValueForAvroType(null);

            expect(result).toBeUndefined();
        });
    });
});