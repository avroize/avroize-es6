
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
    },{
        "default": [],
        "name": "level1a",
        "type": {
            "default": [],
            "type": avroTypes.ARRAY,
            "items": {
                "name": "level1a_data",
                "type": avroTypes.RECORD,
                "fields":[{
                    "default": 1,
                    "name": "field8",
                    "type": avroTypes.INTEGER
                }]
            }
        }
    },{
        "default": null,
        "name": "level1b",
        "type": ["null", {
            "default": [],
            "type": avroTypes.ARRAY,
            "items": {
                "name": "level1b_data",
                "type": avroTypes.RECORD,
                "fields":[{
                    "default": 1,
                    "name": "field9",
                    "type": avroTypes.STRING
                }]
            }
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
            },{
                "default": [],
                "name": "level2a",
                "type": {
                    "default": [],
                    "type": avroTypes.ARRAY,
                    "items": {
                        "name": "level2a_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": 1,
                            "name": "field8",
                            "type": avroTypes.INTEGER
                        },{
                            "name": "level3a",
                            "type": {
                                "name": "level3a_data",
                                "type": avroTypes.RECORD,
                                "fields":[{
                                    "default": true,
                                    "name": "field10",
                                    "type": avroTypes.BOOLEAN
                                }]
                            }
                        }]
                    }
                }
            },{
                "default": null,
                "name": "level2b",
                "type": ["null", {
                    "default": [],
                    "type": avroTypes.ARRAY,
                    "items": {
                        "name": "level2b_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": 1,
                            "name": "field9",
                            "type": avroTypes.STRING
                        }, {
                            "name": "level3b",
                            "type": [null, {
                                "name": "level3b_data",
                                "type": avroTypes.RECORD,
                                "fields":[{
                                    "default": "11",
                                    "name": "field11",
                                    "type": avroTypes.STRING
                                }]
                            }]
                        }]
                    }
                }]
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
            },{
                "default": [],
                "name": "level2a",
                "type": {
                    "default": [],
                    "type": avroTypes.ARRAY,
                    "items": {
                        "name": "level2a_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": 1,
                            "name": "field8",
                            "type": avroTypes.INTEGER
                        },{
                            "name": "level3a",
                            "type": {
                                "name": "level3a_data",
                                "type": avroTypes.RECORD,
                                "fields":[{
                                    "default": true,
                                    "name": "field10",
                                    "type": avroTypes.BOOLEAN
                                }]
                            }
                        }]
                    }
                }
            },{
                "default": null,
                "name": "level2b",
                "type": ["null", {
                    "default": [],
                    "type": avroTypes.ARRAY,
                    "items": {
                        "name": "level2b_data",
                        "type": avroTypes.RECORD,
                        "fields":[{
                            "default": 1,
                            "name": "field9",
                            "type": avroTypes.STRING
                        }, {
                            "name": "level3b",
                            "type": [null, {
                                "name": "level3b_data",
                                "type": avroTypes.RECORD,
                                "fields":[{
                                    "default": "11",
                                    "name": "field11",
                                    "type": avroTypes.STRING
                                }]
                            }]
                        }]
                    }
                }]
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
                    field7: null,
                    level1a: [],
                    level1b: null
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
                        field7: [],
                        level2a: [],
                        level2b: null
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

                expect(result).toHaveLength(9);
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

            test("element 8 level 1a", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("level1a", "array", false, true, [], [
                    new AvroNode("level1", null, false)], [
                        new AvroElement("field8", "int", false, false, 1, [
                            new AvroNode("level1a_data", null, false)
                        ])
                ]);

                expect(result[7]).toEqual(expected);
            });

            test("element 9 level 1b", () => {
                result = Avroizer.getAvroElement(level1AvroSchema, [], []);
                expected = new AvroElement("level1b", "array", true, true, null, [
                    new AvroNode("level1", null, false)], [
                    new AvroElement("field9", "string", false, false, 1, [
                        new AvroNode("level1b_data", null, false)
                    ])
                ]);

                expect(result[8]).toEqual(expected);
            });
        });

        describe("multi-level nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);

                expect(result).toHaveLength(9);
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

            test("element 8 level 2a", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("level2a", "array", false, true, [], [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ], [
                    new AvroElement("field8", "int", false, false, 1, [
                        new AvroNode("level2a_data", null, false)
                    ]), new AvroElement("field10", "boolean", false, false, true, [
                        new AvroNode("level2a_data", null, false),
                        new AvroNode("level3a", "avro.test.level3a_data", false)
                    ])
                ]);

                expect(result[7]).toEqual(expected);
            });

            test("element 9 level 2b", () => {
                result = Avroizer.getAvroElement(level2AvroSchema, [], []);
                expected = new AvroElement("level2b", "array", true, true, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", false)
                ], [
                    new AvroElement("field9", "string", false, false, 1, [
                        new AvroNode("level2b_data", null, false)
                    ]), new AvroElement("field11", "string", false, false, "11", [
                        new AvroNode("level2b_data", null, false),
                        new AvroNode("level3b", "avro.test.level3b_data", true)
                    ])
                ]);

                expect(result[8]).toEqual(expected);
            });
        });

        describe("multi-level nullable nesting", () => {
            test("expected number of elements", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);

                expect(result).toHaveLength(9);
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

            test("element 8 level 2a", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("level2a", "array", false, true, [], [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ], [
                    new AvroElement("field8", "int", false, false, 1, [
                        new AvroNode("level2a_data", null, false)
                    ]), new AvroElement("field10", "boolean", false, false, true, [
                        new AvroNode("level2a_data", null, false),
                        new AvroNode("level3a", "avro.test.level3a_data", false)
                    ])
                ]);

                expect(result[7]).toEqual(expected);
            });

            test("element 9 level 2b", () => {
                result = Avroizer.getAvroElement(level2NullableAvroSchema, [], []);
                expected = new AvroElement("level2b", "array", true, true, null, [
                    new AvroNode("level1", null, false),
                    new AvroNode("level2", "avro.test.level2_data", true)
                ], [
                    new AvroElement("field9", "string", false, false, 1, [
                        new AvroNode("level2b_data", null, false)
                    ]), new AvroElement("field11", "string", false, false, "11", [
                        new AvroNode("level2b_data", null, false),
                        new AvroNode("level3b", "avro.test.level3b_data", true)
                    ])
                ]);

                expect(result[8]).toEqual(expected);
            });
        });
    });
});