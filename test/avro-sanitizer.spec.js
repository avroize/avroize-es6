
jest.mock("../src/utilities");

import sanitize from "../src/avro-sanitizer";
import avroTypes from "../src/constants/avro-types";
import * as utilities from "../src/utilities";

let result;

describe("avroSanitizer", () => {
    describe("sanitize", () => {
        beforeEach(() => {
            jest.resetAllMocks();
        });

        describe("boolean", () => {
            test("return value if boolean", () => {
                utilities.isBoolean
                    .mockReturnValueOnce(true);

                result = sanitize(avroTypes.BOOLEAN, false, true);

                expect(result).toBeTruthy();
            });

            test("return nullable value if boolean", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isBoolean
                    .mockReturnValueOnce(true);
                const avroValue = { boolean: true };

                result = sanitize(avroTypes.BOOLEAN, true, avroValue);

                expect(result).toEqual(avroValue);
            });

            test("getDefaultValueForAvroType from utilities if not boolean", () => {
                utilities.isBoolean
                    .mockReturnValueOnce(false);

                sanitize(avroTypes.BOOLEAN, false, undefined);

                expect(utilities.getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.BOOLEAN);
            });

            test("return null if not valid nullable object", () => {
                utilities.isObject
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce(false);

                result = sanitize(avroTypes.BOOLEAN, true, undefined);

                expect(result).toBeNull();
            });

            test("return null if not valid nullable boolean", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isBoolean
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.BOOLEAN, true, {});

                expect(result).toBeNull();
            });
        });

        describe("double", () => {
            test("return value if double", () => {
                utilities.isDouble
                    .mockReturnValueOnce(true);

                result = sanitize(avroTypes.DOUBLE, false, 1.0);

                expect(result).toEqual(1.0);
            });

            test("return nullable value if double", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isDouble
                    .mockReturnValueOnce(true);
                const avroValue = { float: 1.0 };

                result = sanitize(avroTypes.DOUBLE, true, avroValue);

                expect(result).toEqual(avroValue);
            });

            test("getDefaultValueForAvroType from utilities if not double", () => {
                utilities.isDouble
                    .mockReturnValueOnce(false);

                sanitize(avroTypes.DOUBLE, false, undefined);

                expect(utilities.getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.DOUBLE);
            });

            test("return default value if not double", () => {
                utilities.isDouble
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce(0);

                result = sanitize(avroTypes.DOUBLE, false, undefined);

                expect(result).toEqual(0);
            });

            test("return null if not valid nullable object", () => {
                utilities.isObject
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.DOUBLE, true, undefined);

                expect(result).toBeNull();
            });

            test("return null if not valid nullable double", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isDouble
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.DOUBLE, true, {});

                expect(result).toBeNull();
            });
        });

        describe("float", () => {
            test("return value if float", () => {
                utilities.isFloat
                    .mockReturnValueOnce(true);

                result = sanitize(avroTypes.FLOAT, false, 1.0);

                expect(result).toEqual(1.0);
            });

            test("return nullable value if float", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isFloat
                    .mockReturnValueOnce(true);
                const avroValue = { float: 1.0 };

                result = sanitize(avroTypes.FLOAT, true, avroValue);

                expect(result).toEqual(avroValue);
            });

            test("getDefaultValueForAvroType from utilities if not float", () => {
                utilities.isFloat
                    .mockReturnValueOnce(false);

                sanitize(avroTypes.FLOAT, false, undefined);

                expect(utilities.getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.FLOAT);
            });

            test("return default value if not float", () => {
                utilities.isFloat
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce(0);

                result = sanitize(avroTypes.FLOAT, false, undefined);

                expect(result).toEqual(0);
            });

            test("return null if not valid nullable object", () => {
                utilities.isObject
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.FLOAT, true, undefined);

                expect(result).toBeNull();
            });

            test("return null if not valid nullable float", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isFloat
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.FLOAT, true, {});

                expect(result).toBeNull();
            });
        });

        describe("integer", () => {
            test("return value if integer", () => {
                utilities.isInteger
                    .mockReturnValueOnce(true);

                result = sanitize(avroTypes.INTEGER, false, 1);

                expect(result).toEqual(1);
            });

            test("return nullable value if integer", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isInteger
                    .mockReturnValueOnce(true);
                const avroValue = { int: 1 };

                result = sanitize(avroTypes.INTEGER, true, avroValue);

                expect(result).toEqual(avroValue);
            });

            test("getDefaultValueForAvroType from utilities if not integer", () => {
                utilities.isInteger
                    .mockReturnValueOnce(false);

                sanitize(avroTypes.INTEGER, false, undefined);

                expect(utilities.getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.INTEGER);
            });

            test("return default value if not integer", () => {
                utilities.isInteger
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce(0);

                result = sanitize(avroTypes.INTEGER, false, undefined);

                expect(result).toEqual(0);
            });

            test("return null if not valid nullable object", () => {
                utilities.isObject
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.INTEGER, true, undefined);

                expect(result).toBeNull();
            });

            test("return null if not valid nullable integer", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isInteger
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.INTEGER, true, {});

                expect(result).toBeNull();
            });
        });

        describe("long", () => {
            test("return value if long", () => {
                utilities.isLong
                    .mockReturnValueOnce(true);

                result = sanitize(avroTypes.LONG, false, 1);

                expect(result).toEqual(1);
            });

            test("return nullable value if long", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isLong
                    .mockReturnValueOnce(true);
                const avroValue = { long: 1 };

                result = sanitize(avroTypes.LONG, true, avroValue);

                expect(result).toEqual(avroValue);
            });

            test("getDefaultValueForAvroType from utilities if not long", () => {
                utilities.isLong
                    .mockReturnValueOnce(false);

                sanitize(avroTypes.LONG, false, undefined);

                expect(utilities.getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.LONG);
            });

            test("return default value if not long", () => {
                utilities.isLong
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce(0);

                result = sanitize(avroTypes.LONG, false, undefined);

                expect(result).toEqual(0);
            });

            test("return null if not valid nullable object", () => {
                utilities.isObject
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.LONG, true, undefined);

                expect(result).toBeNull();
            });

            test("return null if not valid nullable long", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isLong
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.LONG, true, {});

                expect(result).toBeNull();
            });
        });

        describe("string", () => {
            test("return value if string", () => {
                utilities.isString
                    .mockReturnValueOnce(true);

                result = sanitize(avroTypes.STRING, false, "1");

                expect(result).toEqual("1");
            });

            test("return nullable value if string", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isString
                    .mockReturnValueOnce(true);
                const avroValue = { string: "1" };

                result = sanitize(avroTypes.STRING, true, avroValue);

                expect(result).toEqual(avroValue);
            });

            test("getDefaultValueForAvroType from utilities if not string", () => {
                utilities.isString
                    .mockReturnValueOnce(false);

                sanitize(avroTypes.STRING, false, undefined);

                expect(utilities.getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.STRING);
            });

            test("return null if not valid nullable object", () => {
                utilities.isObject
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.STRING, true, undefined);

                expect(result).toBeNull();
            });

            test("return null if not valid nullable string", () => {
                utilities.isObject
                    .mockReturnValueOnce(true);
                utilities.isString
                    .mockReturnValueOnce(false);
                utilities.getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.STRING, true, {});

                expect(result).toBeNull();
            });
        });
    });
});
