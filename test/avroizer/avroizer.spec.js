
import _ from "lodash";
import AvroElement from "../../src/avro-objects/avro-element";
import AvroNode from "../../src/avro-objects/avro-node";
import Avroizer from "../../src/avroizer/avroizer";
import {avroTypes} from "../../src/constants/avro-types";

const rootAvroSchema = {
    "default": "1",
    "name": "field1",
    "type": avroTypes.STRING
};

const level1AvroSchema = {
    "name": "level1",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": "1",
        "name": "field1",
        "type": avroTypes.STRING
    },{
        "default": 2,
        "name": "field2",
        "type": avroTypes.INTEGER
    }]
};

const level2AvroSchema = {
    "name": "level1",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1,
        "name": "field1",
        "type": avroTypes.INTEGER
    },{
        "name": "level2",
        "type": {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": 2,
                "name": "field2",
                "type": avroTypes.INTEGER
            },{
                "default": "3",
                "name": "field3",
                "type": avroTypes.STRING
            }]
        }
    }]
};

let expected, result;

describe("Avroizer", () => {
    describe("constructor", () => {
        test("get avroElements returns createAvroElement value", () => {
            result = new Avroizer(rootAvroSchema, null);

            expect(result.avroElements).toEqual(Avroizer.createAvroElement(rootAvroSchema, [], []));
        });

        test("visitors", () => {
            const visitors = [];

            result = new Avroizer({}, visitors);

            expect(result.visitors).toBe(visitors);
        });
    });

    describe("avroize", () => {
        test("cloneDeep from lodash", () => {
            const avroizer = new Avroizer(rootAvroSchema, null);
            const cloneDeepSpy = jest.spyOn(_, "cloneDeep");

            avroizer.avroize({});

            expect(cloneDeepSpy).toHaveBeenCalled();
        });

        test("accept each visitor", () => {
            const visitor = {
                visit: () => {
                    return undefined;
                }
            };
            const visitors = [visitor, visitor];
            const avroizer = new Avroizer(rootAvroSchema, visitors);
            const data = {};
            const acceptSpy = jest.spyOn(avroizer.avroElements[0], "accept");

            avroizer.avroize(data);

            expect(acceptSpy).toHaveBeenCalledWith(visitor, data);
            expect(acceptSpy.mock.calls.length).toEqual(2);
        });

        describe("root level", () => {
            test("empty data object", () => {
                const avroizer = new Avroizer(rootAvroSchema, null);

                result = avroizer.avroize({});
                expected = "";

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