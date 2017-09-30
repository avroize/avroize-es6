
jest.mock("../../src/avro-sanitizer");

import JSONVisitor from "../../src/visitors/json-visitor";
import AvroElement from "../../src/avro-objects/avro-element";
import AvroNode from "../../src/avro-objects/avro-node";
import avroTypes from "../../src/constants/avro-types";
import sanitize from "../../src/avro-sanitizer";

let avroElement, data, parentNodes, result, visitor;

describe("JSONVisitor", () => {
    describe("visit", () => {
        beforeEach(() => {
            visitor = new JSONVisitor();
            jest.resetAllMocks();
        });

        describe("element is not record", () => {
            describe("no parent", () => {
                test("request sanitize from utilities", () => {
                    avroElement = new AvroElement("field1", avroTypes.STRING, false, "", []);
                    data = {};

                    visitor.visit(avroElement, data);

                    expect(sanitize).toHaveBeenCalledWith(avroElement.dataType, false, data);
                });

                test("set sanitized data for element", () => {
                    avroElement = new AvroElement("field1", avroTypes.STRING, false, "", []);
                    data = "1";
                    sanitize.
                        mockReturnValueOnce(data);

                    visitor.visit(avroElement, data);

                    expect(avroElement.value).toEqual(data);
                });
            });

            describe("has parent", () => {
                test("request sanitize from utilities for element if property exists", () => {
                    const rootNode = new AvroNode("root", null, false);
                    parentNodes = [rootNode];
                    avroElement = new AvroElement("field1", avroTypes.STRING, false, "", parentNodes);
                    data = {
                        field1: "1"
                    };

                    visitor.visit(avroElement, data);

                    expect(sanitize).toHaveBeenCalled();
                });

                test("set sanitized data for element if property exists", () => {
                    const rootNode = new AvroNode("root", null, false);
                    parentNodes = [rootNode];
                    avroElement = new AvroElement("field1", avroTypes.STRING, false, "", parentNodes);
                    data = {
                        field1: "1"
                    };

                    sanitize.
                        mockReturnValueOnce("1");

                    visitor.visit(avroElement, data);

                    expect(avroElement.value).toEqual(data.field1);
                });

                test("not request sanitize from utilities for element if data object does not exist", () => {
                    const rootNode = new AvroNode("root", null, false);
                    parentNodes = [rootNode];
                    avroElement = new AvroElement("field1", avroTypes.STRING, false, "", parentNodes);
                    data = undefined;

                    visitor.visit(avroElement, data);

                    expect(sanitize).not.toHaveBeenCalled();
                });

                test("not request sanitize from utilities for element if property does not exist", () => {
                    const rootNode = new AvroNode("root", null, false);
                    parentNodes = [rootNode];
                    avroElement = new AvroElement("field1", avroTypes.STRING, false, "", parentNodes);
                    data = {};

                    visitor.visit(avroElement, data);

                    expect(sanitize).not.toHaveBeenCalled();
                });
            });
        });

        test("do nothing if element is a record", () => {
            avroElement = new AvroElement("field1", avroTypes.RECORD, false, "", []);
            data = {};

            visitor.visit(avroElement, data);

            expect(sanitize).not.toHaveBeenCalled();
        });
    });

    describe("getCurrentObject", () => {
        test("return data if no parent exists", () => {
            parentNodes = [];
            data = {};

            result = JSONVisitor.getCurrentObject(parentNodes, data);

            expect(result).toBe(data);
        });

        test("return undefined if parents exist and data is not an object", () => {
            const level1Node = new AvroNode("level1", null, false);
            parentNodes = [level1Node];
            data = undefined;

            result = JSONVisitor.getCurrentObject(parentNodes, data);

            expect(result).toBeUndefined();
        });

        test("return parent object from data for single parent", () => {
            const level1Node = new AvroNode("level1", null, false);
            parentNodes = [level1Node];
            data = {
              level1: {}
            };

            result = JSONVisitor.getCurrentObject(parentNodes, data);

            expect(result).toBe(data.level1);
        });

        test("return last parent object from data for multiple parent", () => {
            const level1Node = new AvroNode("level1", null, false);
            const level2Node = new AvroNode("level2", null, false);
            parentNodes = [level1Node, level2Node];
            data = {
                level1: {
                    level2: {}
                }
            };

            result = JSONVisitor.getCurrentObject(parentNodes, data);

            expect(result).toBe(data.level1.level2);
        });
    });
});

