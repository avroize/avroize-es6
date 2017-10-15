import JSONAvroizer from "./avroizer/json-avroizer";
import {isObject} from "./utilities";

export const INVALID_SCHEMA = "Avro schema is not a valid object";

export function getJSONAvroizer(avroSchema) {
    if (!isObject(avroSchema)) {
        throw Error(INVALID_SCHEMA);
    }

    return new JSONAvroizer(avroSchema);
}