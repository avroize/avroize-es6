
import JSONAvroizer from "../../src/avroizer/json-avroizer";
import Avroizer from "../../src/avroizer/avroizer";
import JSONVisitor from "../../src/visitors/json-visitor";

describe("JSONAvroizer", () => {
    describe("constructor", () => {
        test("set JSONVisitor as visitor", () => {
            const avroizer = new JSONAvroizer({});

            expect(avroizer.visitors[0]).toBeInstanceOf(JSONVisitor);
        });

        test("subclass Avroizer", () => {
            const avroizer = new JSONAvroizer({});

            expect(avroizer).toBeInstanceOf(Avroizer);
        });
    });
});