
import Avroizer from "./avroizer";
import JSONVisitor from "../visitors/json-visitor";

export default class JSONAvroizer extends Avroizer {
    constructor(avroSchema) {
        const visitor = new JSONVisitor();
        super(avroSchema, [visitor]);
    }
}
