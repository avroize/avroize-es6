
import AvroElement from "../../src/avro-objects/AvroElement";
import AvroNode from "../../src/avro-objects/AvroNode";
import Avroizer from "../../src/avroizer/Avroizer";
import {avroTypes} from "../../src/constants/AvroTypes";

const rootAvroSchema = {
    default: "1",
    name: "field1",
    type: "string"
};

const level1AvroSchema = {
    name: "level1",
    type: "record",
    fields:[{
        default: "1",
        name: "field1",
        type: "string"
    },{
        default: 2,
        name: "field2",
        type: "int"
    }]
};

const level2AvroSchema = {
    name: "level1",
    type: "record",
    fields:[{
        default: 1,
        name: "field1",
        type: "int"
    },{
        name: "level2",
        type: {
            name: "level2_data",
            type: "record",
            fields:[{
                default: 2,
                name: "field2",
                type: "int"
            },{
                default: "3",
                name: "field3",
                type: "string"
            }]
        }
    }]
};

let expected, result;

describe("Avroizer", () => {
    describe("avroize", () => {
        describe("root level", () => {
            test("empty data object", () => {
                const avroizer = new Avroizer(rootAvroSchema, null);

                result = avroizer.avroize({});
                expected = {
                    field1: ""
                };

                expect(result).toEqual(expected);
            });
        });

        describe("one level", () => {
            test("empty data object", () => {
                const avroizer = new Avroizer(level1AvroSchema, null);

                result = avroizer.avroize({});
                expected = {
                    field1: "",
                    field2: 0
                };

                expect(result).toEqual(expected);
            });
        });

        describe("multi-level", () => {
            test("empty data object", () => {
                const avroizer = new Avroizer(level2AvroSchema, null);

                result = avroizer.avroize({});
                expected = {
                    field1: 0,
                    level2: {
                        field2: 0,
                        field3: ""
                    }
                };

                expect(result).toEqual(expected);
            });
        });
    });

    describe("createAvroElement", () => {
        describe("root level", () => {
            test("element 1 that is not a record", () => {
                result = Avroizer.createAvroElement(rootAvroSchema, [], []);
                expected = new AvroElement("field1", "string", "1", []);

                expect(result[0]).toEqual(expected);
            });

            test("expected number of elements", () => {
                result = Avroizer.createAvroElement(rootAvroSchema, [], []);

                expect(result.length).toEqual(1);
            });
        });

        describe("one level nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.createAvroElement(level1AvroSchema, [], []);

                expect(result.length).toEqual(2);
            });

            test("element 1 that is not a record", () => {
                result = Avroizer.createAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field1", "string", "1",
                    [new AvroNode("level1", null, false)]);

                expect(result[0]).toEqual(expected);
            });

            test("element 2 that is not a record", () => {
                result = Avroizer.createAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field2", "int", 2,
                    [new AvroNode("level1", null, false)]);

                expect(result[1]).toEqual(expected);
            });
        });

        describe("multi-level nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.createAvroElement(level2AvroSchema, [], []);

                expect(result.length).toEqual(3);
            });

            test("element 1 that is not a record", () => {
                result = Avroizer.createAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field1", "int", 1,
                    [new AvroNode("level1", null, false)]);

                expect(result[0]).toEqual(expected);
            });

            test("element 2 that is not a record", () => {
                result = Avroizer.createAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field2", "int", 2, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "level2_data", false)
                ]);

                expect(result[1]).toEqual(expected);
            });

            test("element 3 that is not a record", () => {
                result = Avroizer.createAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field3", "string", "3", [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "level2_data", false)
                ]);

                expect(result[2]).toEqual(expected);
            });
        });
    });
});