
jest.mock("../src/utilities");

import sanitize from "../src/avro-sanitizer";
import {avroTypes} from "../src/constants/avro-types";
import {getDefaultValueForAvroType, isInteger, isString} from "../src/utilities";

let result;

describe("avroSanitizer", () => {
    describe("sanitize", () => {
        describe("string", () => {
            test("return value if string", () => {
                isString.
                    mockReturnValueOnce(true);

                result = sanitize(avroTypes.STRING, "1");

                expect(result).toEqual("1");
            });

            test("getDefaultValueForAvroType from utilities if not string", () => {
                isString.
                    mockReturnValueOnce(false);

                sanitize(avroTypes.STRING, undefined);

                expect(getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.STRING);
            });

            test("return default value if not string", () => {
                isString.
                    mockReturnValueOnce(false);

                getDefaultValueForAvroType
                    .mockReturnValueOnce("");

                result = sanitize(avroTypes.STRING, undefined);

                expect(result).toEqual("");
            });
        });

        describe("integer", () => {
            test("return value if integer", () => {
                isInteger.
                    mockReturnValueOnce(true);

                result = sanitize(avroTypes.INTEGER, 1);

                expect(result).toEqual(1);
            });

            test("getDefaultValueForAvroType from utilities if not integer", () => {
                isInteger.
                    mockReturnValueOnce(false);

                sanitize(avroTypes.INTEGER, undefined);

                expect(getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.INTEGER);
            });

            test("return default value if not integer", () => {
                isInteger.
                    mockReturnValueOnce(false);

                getDefaultValueForAvroType
                    .mockReturnValueOnce(0);

                result = sanitize(avroTypes.INTEGER, undefined);

                expect(result).toEqual(0);
            });
        });
    });
});
