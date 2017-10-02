
import _ from "lodash";
import AvroElement from "../../src/avro-objects/avro-element";
import AvroNode from "../../src/avro-objects/avro-node";
import Avroizer from "../../src/avroizer/avroizer";
import avroTypes from "../../src/constants/avro-types";

const rootAvroSchema = {
    "default": "1",
    "name": "field1",
    "type": avroTypes.STRING
};

const level1AvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": null,
        "name": "field1",
        "type": [avroTypes.NULL, avroTypes.STRING]
    },{
        "default": 2,
        "name": "field2",
        "type": avroTypes.INTEGER
    },{
        "default": true,
        "name": "field3",
        "type": avroTypes.BOOLEAN
    },{
        "default": null,
        "name": "field4",
        "type": [avroTypes.NULL, avroTypes.DOUBLE]
    },{
        "default": 10000,
        "name": "field5",
        "type": avroTypes.LONG
    },{
        "default": 1.1,
        "name": "field6",
        "type": avroTypes.FLOAT
    },{
        "default": null,
        "name": "field7",
        "type": [avroTypes.NULL, {
            "type": avroTypes.ARRAY,
            "items": avroTypes.DOUBLE
        }]
    }]
};

const level2AvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1.1,
        "name": "field1",
        "type": avroTypes.DOUBLE
    },{
        "default": null,
        "name": "field2",
        "type": ["null", avroTypes.LONG]
    },{
        "name": "level2",
        "type": {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": null,
                "name": "field3",
                "type": [avroTypes.NULL, avroTypes.FLOAT]
            },{
                "default": "4",
                "name": "field4",
                "type": avroTypes.STRING
            },{
                "default": null,
                "name": "field5",
                "type": [avroTypes.NULL, avroTypes.BOOLEAN]
            },{
                "default": null,
                "name": "field6",
                "type": [avroTypes.NULL, avroTypes.INTEGER]
            },{
                "default": [],
                "name": "field7",
                "type": {
                    "type": avroTypes.ARRAY,
                    "items": avroTypes.LONG
                }
            }]
        }
    }]
};

const level2NullableAvroSchema = {
    "name": "level1",
    "namespace": "avro.test",
    "type": avroTypes.RECORD,
    "fields":[{
        "default": 1.1,
        "name": "field1",
        "type": avroTypes.DOUBLE
    },{
        "default": null,
        "name": "field2",
        "type": ["null", avroTypes.LONG]
    },{
        "name": "level2",
        "type": [avroTypes.NULL, {
            "name": "level2_data",
            "type": avroTypes.RECORD,
            "fields":[{
                "default": null,
                "name": "field3",
                "type": [avroTypes.NULL, avroTypes.FLOAT]
            },{
                "default": "4",
                "name": "field4",
                "type": avroTypes.STRING
            },{
                "default": null,
                "name": "field5",
                "type": [avroTypes.NULL, avroTypes.BOOLEAN]
            },{
                "default": null,
                "name": "field6",
                "type": [avroTypes.NULL, avroTypes.INTEGER]
            },{
                "default": [],
                "name": "field7",
                "type": {
                    "type": avroTypes.ARRAY,
                    "items": avroTypes.LONG
                }
            }]
        }]
    }]
};

let expected, result;

describe("Avroizer", () => {
    describe("constructor", () => {
        test("get avroElements returns createAvroElement value", () => {
            result = new Avroizer(rootAvroSchema, null);

            expect(result.avroElements).toEqual(Avroizer.getAvroElement(rootAvroSchema, [], []));
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
            expect(acceptSpy.mock.calls).toHaveLength(2);
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
                    field1: null,
                    field2: 0,
                    field3: false,
                    field4: null,
                    field5: 0,
                    field6: 0,
                    field7: null
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
                    field2: null,
                    level2: {
                        field3: null,
                        field4: "",
                        field5: null,
                        field6: null,
                        field7: []
                    }
                };

                expect(result).toEqual(expected);
            });
        });

        describe("multi-level nullable", () => {
            test("empty data object", () => {
                const avroizer = new Avroizer(level2NullableAvroSchema, null);

                result = avroizer.avroize({});
                expected = {
                    field1: 0,
                    field2: null,
                    level2: null
                };

                expect(result).toEqual(expected);
            });
        });
    });

    describe("getAvroElement", () => {
        describe("root level", () => {
            test("element 1 root level", () => {
                result = Avroizer.getAvroElement(rootAvroSchema, [], []);
                expected = new AvroElement("field1", "string", false, false, "1", []);

                expect(result[0]).toEqual(expected);
            });

            test("expected number of elements", () => {
                result = Avroizer.getAvroElement(rootAvroSchema, [], []);

                expect(result).toHaveLength(1);
            });
        });

        describe("one level nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);

                expect(result).toHaveLength(7);
            });

            test("element 1 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field1", "string", true, false, null,
                    [new AvroNode("level1", null, false)]);

                expect(result[0]).toEqual(expected);
            });

            test("element 2 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field2", "int", false, false, 2,
                    [new AvroNode("level1", null, false)]);

                expect(result[1]).toEqual(expected);
            });

            test("element 3 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field3", "boolean", false, false, true,
                    [new AvroNode("level1", null, false)]);

                expect(result[2]).toEqual(expected);
            });

            test("element 4 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field4", "double", true, false, null,
                    [new AvroNode("level1", null, false)]);

                expect(result[3]).toEqual(expected);
            });

            test("element 5 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field5", "long", false, false, 10000,
                    [new AvroNode("level1", null, false)]);

                expect(result[4]).toEqual(expected);
            });

            test("element 6 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field6", "float", false, false, 1.1,
                    [new AvroNode("level1", null, false)]);

                expect(result[5]).toEqual(expected);
            });

            test("element 7 level 1", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("field7", "double", true, true, null,
                    [new AvroNode("level1", null, false)]);

                expect(result[6]).toEqual(expected);
            });
        });

        describe("multi-level nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);

                expect(result).toHaveLength(7);
            });

            test("element 1 level 1", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field1", "double", false, false, 1.1,
                    [new AvroNode("level1", null, false)]);

                expect(result[0]).toEqual(expected);
            });

            test("element 2 level 1", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field2", "long", true, false, null,
                    [new AvroNode("level1", null, false)]);

                expect(result[1]).toEqual(expected);
            });

            test("element 3 level 2", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field3", "float", true, false, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ]);

                expect(result[2]).toEqual(expected);
            });

            test("element 4 level 2", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field4", "string", false, false, "4", [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ]);

                expect(result[3]).toEqual(expected);
            });

            test("element 5 level 2", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field5", "boolean", true, false, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ]);

                expect(result[4]).toEqual(expected);
            });

            test("element 6 level 2", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field6", "int", true, false, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ]);

                expect(result[5]).toEqual(expected);
            });

            test("element 7 level 2", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("field7", "long", false, true, [], [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ]);

                expect(result[6]).toEqual(expected);
            });
        });

        describe("multi-level nullable nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);

                expect(result).toHaveLength(7);
            });

            test("element 1 level 1", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field1", "double", false, false, 1.1,
                    [new AvroNode("level1", null, false)]);

                expect(result[0]).toEqual(expected);
            });

            test("element 2 level 1", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field2", "long", true, false, null,
                    [new AvroNode("level1", null, false)]);

                expect(result[1]).toEqual(expected);
            });

            test("element 3 level 2", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field3", "float", true, false, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ]);

                expect(result[2]).toEqual(expected);
            });

            test("element 4 level 2", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field4", "string", false, false, "4", [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ]);

                expect(result[3]).toEqual(expected);
            });

            test("element 5 level 2", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field5", "boolean", true, false, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ]);

                expect(result[4]).toEqual(expected);
            });

            test("element 6 level 2", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field6", "int", true, false, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ]);

                expect(result[5]).toEqual(expected);
            });

            test("element 7 level 2", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("field7", "long", false, true, [], [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ]);

                expect(result[6]).toEqual(expected);
            });
        });
    });
});