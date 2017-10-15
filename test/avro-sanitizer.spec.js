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

        test("getDefaultValueForAvroType from utilities", () => {
            sanitize(avroTypes.DOUBLE, true, true, undefined);

            expect(utilities.getDefaultValueForAvroType)
                .toHaveBeenCalledWith(avroTypes.DOUBLE, true, true);
        });

        test("return value if double", () => {
            utilities.isValidPrimitive
                .mockReturnValueOnce(true);

            result = sanitize(avroTypes.DOUBLE, false, false, 1.0);

            expect(result).toEqual(1.0);
        });

        test("return wrapped value if double", () => {
            utilities.isObject
                .mockReturnValueOnce(true);
            utilities.isValidPrimitive
                .mockReturnValueOnce(true);
            const avroValue = { float: 1.0 };

            result = sanitize(avroTypes.DOUBLE, true, false, avroValue);

            expect(result).toEqual(avroValue);
        });

        test("return default value if not double", () => {
            utilities.isValidPrimitive
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce(0);

            result = sanitize(avroTypes.DOUBLE, false, false, undefined);

            expect(result).toEqual(0);
        });

        test("return default if not valid wrapped object", () => {
            utilities.isObject
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce(null);

            result = sanitize(avroTypes.DOUBLE, true, false, undefined);

            expect(result).toBeNull();
        });

        test("return default if not valid wrapped double", () => {
            utilities.isObject
                .mockReturnValueOnce(true);
            utilities.isValidPrimitive
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce(null);

            result = sanitize(avroTypes.DOUBLE, true, false, {});

            expect(result).toBeNull();
        });

        test("return array with item if item is valid", () => {
            utilities.isArray
                .mockReturnValueOnce(true);
            utilities.isValidPrimitive
                .mockReturnValueOnce(true);

            const array = [1.1];
            result = sanitize(avroTypes.DOUBLE, false, true, array);

            expect(result).toEqual(array);
        });

        test("return default if item is not valid", () => {
            utilities.isArray
                .mockReturnValueOnce(true);
            utilities.isValidPrimitive
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce([]);

            result = sanitize(avroTypes.DOUBLE, false, true, [undefined]);

            expect(result).toEqual([]);
        });

        test("return default if not array", () => {
            utilities.isArray
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce([]);

            result = sanitize(avroTypes.DOUBLE, false, true, undefined);

            expect(result).toEqual([]);
        });

        test("return wrapped array with item if item is valid", () => {
            utilities.isObject
                .mockReturnValueOnce(true);
            utilities.isArray
                .mockReturnValueOnce(true);
            utilities.isValidPrimitive
                .mockReturnValueOnce(true);

            const array = { array: [1.1] };
            result = sanitize(avroTypes.DOUBLE, true, true, array);

            expect(result).toEqual(array);
        });

        test("return wrapped empty array if item is not valid", () => {
            utilities.isObject
                .mockReturnValueOnce(true);
            utilities.isArray
                .mockReturnValueOnce(true);
            utilities.isValidPrimitive
                .mockReturnValueOnce(false);

            result = sanitize(avroTypes.DOUBLE, true, true, { array: [undefined] });

            expect(result).toEqual({ array: [] });
        });

        test("return default if not a wrappable array", () => {
            utilities.isObject
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce(null);

            result = sanitize(avroTypes.DOUBLE, true, true, undefined);

            expect(result).toBeNull();
        });

        test("return default if not valid wrappable array", () => {
            utilities.isObject
                .mockReturnValueOnce(true);
            utilities.isArray
                .mockReturnValueOnce(false);
            utilities.getDefaultValueForAvroType
                .mockReturnValueOnce(null);

            result = sanitize(avroTypes.DOUBLE, true, true, { array: undefined });

            expect(result).toBeNull();
        });
    });
});
