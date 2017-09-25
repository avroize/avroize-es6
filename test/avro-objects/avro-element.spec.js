
jest.mock("../../src/utilities");

import AvroElement from "../../src/avro-objects/avro-element";
import AvroNode from "../../src/avro-objects/avro-node";
import {avroTypes} from "../../src/constants/avro-types";
import {getDefaultValueForAvroType} from "../../src/utilities";

let result;

describe("AvroElement", () => {
    describe ("properties", () => {
        test("get dataType returns constructor dataType value", () => {
            result = new AvroElement(null, avroTypes.STRING, null, null);

            expect(result.dataType).toEqual(avroTypes.STRING);
        });

        test("get defaultValue returns constructor defaultValue value", () => {
            result = new AvroElement(null, null, "1", null);

            expect(result.defaultValue).toEqual("1");
        });

        test("get name returns constructor name value", () => {
            result = new AvroElement("root", null, null, null);

            expect(result.name).toEqual("root");
        });

        test("get parentNodes returns constructor parentNodes value", () => {
            result = new AvroElement(null, null, null, new AvroNode("root", null, false));

            expect(result.parentNodes).toEqual(new AvroNode("root", null, false));
        });

        test("getDefaultValueForAvroType from utilities", () => {
            result = new AvroElement(null, avroTypes.STRING, null, null);

            expect(getDefaultValueForAvroType).toHaveBeenCalledWith(avroTypes.STRING);
        });
    });

    describe("accept", () => {
        test("should request visit from visitor param", () => {
            const avroElement = new AvroElement("root", avroTypes.STRING, "1", null);
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