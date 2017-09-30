
jest.mock("../../src/utilities");

import AvroElement from "../../src/avro-objects/avro-element";
import AvroNode from "../../src/avro-objects/avro-node";
import avroTypes from "../../src/constants/avro-types";
import {getDefaultValueForAvroType} from "../../src/utilities";

let result;

describe("AvroElement", () => {
    describe ("properties", () => {
        test("get dataType returns constructor dataType value", () => {
            result = new AvroElement(null, avroTypes.STRING, false, null, null);

            expect(result.dataType).toEqual(avroTypes.STRING);
        });

        test("get defaultValue returns constructor defaultValue value", () => {
            result = new AvroElement(null, null, false, "1", null);

            expect(result.defaultValue).toEqual("1");
        });

        test("get isNullable returns constructor isNullable value", () => {
            result = new AvroElement(null, null, true, null, null);

            expect(result.isNullable).toBeTruthy();
        });

        test("get name returns constructor name value", () => {
            result = new AvroElement("root", null, false, null, null);

            expect(result.name).toEqual("root");
        });

        test("get parentNodes returns constructor parentNodes value", () => {
            result = new AvroElement(null, null, false, null, new AvroNode("root", null, false));

            expect(result.parentNodes).toEqual(new AvroNode("root", null, false));
        });

        test("getDefaultValueForAvroType from utilities if not nullable", () => {
            result = new AvroElement(null, avroTypes.STRING, false, null, null);

            expect(getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.STRING, false);
        });

        test("get value returns default value", () => {
            getDefaultValueForAvroType.mockReturnValueOnce("");

            result = new AvroElement(null, null, false, null, null);

            expect(result.value).toEqual("");
        });

        test("get value returns null if nullable", () => {
            result = new AvroElement(null, null, true, null, null);

            expect(result.value).toBeNull();
        });
    });

    describe("accept", () => {
        test("should request visit from visitor param", () => {
            const avroElement = new AvroElement("root", avroTypes.STRING, false, "1", null);
            const data = {};
            const visitor = {
                visit: () => {
                    return undefined;
                }
            };
            const visitSpy = jest.spyOn(visitor, "visit");

            avroElement.accept(visitor, data);

            expect(visitSpy).toHaveBeenCalledWith(avroElement, data);
        });
    });
});