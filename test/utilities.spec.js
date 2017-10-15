import {isArray, isDefined, isObject, isValidPrimitive, getDefaultValueForAvroType} from "../src/utilities";
import avroTypes from "../src/constants/avro-types";

let result;

describe("utilities", () => {
    describe("isArray", () => {
        test("return true if value is an array", () => {
            result = isArray([]);

            expect(result).toBeTruthy();
        });

        test("return false if value is otherwise", () => {
            result = isArray(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isDefined", () => {
        test("return false if value is undefined", () => {
            result = isDefined(undefined);

            expect(result).toBeFalsy();
        });

        test("return true if value is otherwise", () => {
            result = isDefined("value");

            expect(result).toBeTruthy();
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

        test("return false if value is array", () => {
            result = isObject([]);

            expect(result).toBeFalsy();
        });

        test("return false if value is otherwise", () => {
            result = isObject(undefined);

            expect(result).toBeFalsy();
        });
    });

    describe("isValidPrimitive", () => {
        describe("boolean", () => {
            test("return true if value is boolean", () => {
                result = isValidPrimitive(avroTypes.BOOLEAN, true);

                expect(result).toBeTruthy();
            });

            test("return false if value is otherwise", () => {
                result = isValidPrimitive(avroTypes.BOOLEAN, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("double", () => {
            test("return true if value is float", () => {
                result = isValidPrimitive(avroTypes.DOUBLE, 3.1);

                expect(result).toBeTruthy();
            });

            test("return false if value is otherwise", () => {
                result = isValidPrimitive(avroTypes.DOUBLE, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("float", () => {
            test("return true if value is float", () => {
                result = isValidPrimitive(avroTypes.FLOAT, 3.1);

                expect(result).toBeTruthy();
            });

            test("return false if value is otherwise", () => {
                result = isValidPrimitive(avroTypes.FLOAT, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("integer", () => {
            test("return true if value is integer", () => {
                result = isValidPrimitive(avroTypes.INTEGER, 3);

                expect(result).toBeTruthy();
            });

            test("return false if value is float", () => {
                result = isValidPrimitive(avroTypes.INTEGER, 3.1);

                expect(result).toBeFalsy();
            });

            test("return false if value is otherwise", () => {
                result = isValidPrimitive(avroTypes.INTEGER, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("long", () => {
            test("return true if value is integer", () => {
                result = isValidPrimitive(avroTypes.LONG, 3);

                expect(result).toBeTruthy();
            });

            test("return false if value is float", () => {
                result = isValidPrimitive(avroTypes.LONG, 3.1);

                expect(result).toBeFalsy();
            });

            test("return false if value is otherwise", () => {
                result = isValidPrimitive(avroTypes.LONG, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("string", () => {
            test("return true if value is string", () => {
                result = isValidPrimitive(avroTypes.STRING, "");

                expect(result).toBeTruthy();
            });

            test("return false if value is otherwise", () => {
                result = isValidPrimitive(avroTypes.STRING, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("default", () => {
            test("return false", () => {
                result = isValidPrimitive(avroTypes.RECORD, undefined);

                expect(result).toBeFalsy();
            });
        });
    });

    describe("getDefaultValueForAvroType", () => {
        test("is nullable returns null", () => {
            result = getDefaultValueForAvroType(avroTypes.STRING, true);

            expect(result).toBeNull();
        });

        test("is array returns empty array", () => {
            result = getDefaultValueForAvroType(avroTypes.STRING, false, true);

            expect(result).toEqual([]);
        });

        test("boolean avro type returns false", () => {
            result = getDefaultValueForAvroType(avroTypes.BOOLEAN);

            expect(result).toBeFalsy();
        });

        test("double avro type returns zero", () => {
            result = getDefaultValueForAvroType(avroTypes.DOUBLE);

            expect(result).toEqual(0);
        });

        test("float avro type returns zero", () => {
            result = getDefaultValueForAvroType(avroTypes.FLOAT);

            expect(result).toEqual(0);
        });

        test("integer avro type returns zero", () => {
            result = getDefaultValueForAvroType(avroTypes.INTEGER);

            expect(result).toEqual(0);
        });

        test("long avro type returns zero", () => {
            result = getDefaultValueForAvroType(avroTypes.LONG);

            expect(result).toEqual(0);
        });

        test("string avro type returns empty string", () => {
            result = getDefaultValueForAvroType(avroTypes.STRING);

            expect(result).toEqual("");
        });

        test("default returns undefined", () => {
            result = getDefaultValueForAvroType(null);

            expect(result).toBeUndefined();
        });
    });
});