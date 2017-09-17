
import AvroNode from "../../src/avro-objects/AvroNode";

let result;

describe("AvroNode", () => {
    describe ("properties", () => {
        test("get dataNodeName returns constructor dataNodeName value", () => {
            result = new AvroNode(null, "root_data", null);

            expect(result.dataNodeName).toEqual("root_data");
        });

        test("get isNullable returns constructor isNullable value", () => {
            result = new AvroNode(null, null, true);

            expect(result.isNullable).toEqual(true);
        });

        test("get name returns constructor name value", () => {
            result = new AvroNode("root", null, null);

            expect(result.name).toEqual("root");
        });
    });
});