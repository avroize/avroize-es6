
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

    describe("isValidPrimitive", () => {
        describe("boolean", () => {
            test("return true if value is boolean", () => {
                result = utilities.isValidPrimitive(avroTypes.BOOLEAN, true);

                expect(result).toBeTruthy();
            });

            test("return false if value is otherwise", () => {
                result = utilities.isValidPrimitive(avroTypes.BOOLEAN, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("double", () => {
            test("return true if value is float", () => {
                result = utilities.isValidPrimitive(avroTypes.DOUBLE, 3.1);

                expect(result).toBeTruthy();
            });

            test("return false if value is integer", () => {
                result = utilities.isValidPrimitive(avroTypes.DOUBLE, 3);

                expect(result).toBeFalsy();
            });

            test("return false if value is otherwise", () => {
                result = utilities.isValidPrimitive(avroTypes.DOUBLE, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("float", () => {
            test("return true if value is float", () => {
                result = utilities.isValidPrimitive(avroTypes.FLOAT, 3.1);

                expect(result).toBeTruthy();
            });

            test("return false if value is integer", () => {
                result = utilities.isValidPrimitive(avroTypes.FLOAT, 3);

                expect(result).toBeFalsy();
            });

            test("return false if value is otherwise", () => {
                result = utilities.isValidPrimitive(avroTypes.FLOAT, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("integer", () => {
            test("return true if value is integer", () => {
                result = utilities.isValidPrimitive(avroTypes.INTEGER, 3);

                expect(result).toBeTruthy();
            });

            test("return false if value is float", () => {
                result = utilities.isValidPrimitive(avroTypes.INTEGER, 3.1);

                expect(result).toBeFalsy();
            });

            test("return false if value is otherwise", () => {
                result = utilities.isValidPrimitive(avroTypes.INTEGER, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("long", () => {
            test("return true if value is integer", () => {
                result = utilities.isValidPrimitive(avroTypes.LONG, 3);

                expect(result).toBeTruthy();
            });

            test("return false if value is float", () => {
                result = utilities.isValidPrimitive(avroTypes.LONG, 3.1);

                expect(result).toBeFalsy();
            });

            test("return false if value is otherwise", () => {
                result = utilities.isValidPrimitive(avroTypes.LONG, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("string", () => {
            test("return true if value is string", () => {
                result = utilities.isValidPrimitive(avroTypes.STRING, "");

                expect(result).toBeTruthy();
            });

            test("return false if value is otherwise", () => {
                result = utilities.isValidPrimitive(avroTypes.STRING, undefined);

                expect(result).toBeFalsy();
            });
        });

        describe("default", () => {
            test("return false", () => {
                result = utilities.isValidPrimitive(avroTypes.RECORD, undefined);

                expect(result).toBeFalsy();
            });
        });
    });

    describe("getDefaultValueForAvroType", () => {
        test("is nullable returns null", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.STRING, true);

            expect(result).toBeNull();
        });

        test("is array returns empty array", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.STRING, false, true);

            expect(result).toEqual([]);
        });

        test("boolean avro type returns false", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.BOOLEAN);

            expect(result).toBeFalsy();
        });

        test("double avro type returns zero", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.DOUBLE);

            expect(result).toEqual(0);
        });

        test("float avro type returns zero", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.FLOAT);

            expect(result).toEqual(0);
        });

        test("integer avro type returns zero", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.INTEGER);

            expect(result).toEqual(0);
        });

        test("long avro type returns zero", () => {
            result = utilities.getDefaultValueForAvroType(avroTypes.LONG);

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