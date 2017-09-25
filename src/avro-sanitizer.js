
import {avroTypes} from "./constants/avro-types";
import {getDefaultValueForAvroType, isInteger, isString} from "./utilities";

export default function sanitize(type, value) {
    let result;

    switch (type) {
        case avroTypes.INTEGER:
            result = sanitizeInteger(value);
            break;
        case avroTypes.STRING:
            result = sanitizeString(value);
            break;
    }

    return result;
}

function sanitizeString(value) {
    // if int, get string value

    // if not string, return empty string value

    let result;

    // else return value
    if (isString(value)) {
        result = value;
    } else {
        result = getDefaultValueForAvroType(avroTypes.STRING);
    }

    return result;
}

function sanitizeInteger(value) {
    // if string, try return parsed value

    // if not integer. return 0 int value

    let result;

    // else return value
    if (isInteger(value)) {
        result = value;
    } else {
        result = getDefaultValueForAvroType(avroTypes.INTEGER);
    }

    return result;
}