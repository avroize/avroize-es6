
import avroTypes from "./constants/avro-types";

export function isDefined(value) {
    return typeof value !== "undefined" && value !== null;
}

export function isArray(value) {
    return Array.isArray(value);
}

export function isBoolean(value) {
    return typeof value === "boolean";
}

export function isDouble(value) {
    // TODO: check to see if float our double
    return typeof value === "number" && !Number.isInteger(value);
}

export function isFloat(value) {
    // TODO: check to see if float our double
    return typeof value === "number" && !Number.isInteger(value);
}

export function isInteger(value) {
    return typeof value === "number" && Number.isInteger(value);
}

export function isLong(value) {
    // TODO: check to see if integer or long
    return typeof value === "number" && Number.isInteger(value);
}

export function isObject(value) {
    return value !== null && !this.isArray(value) && typeof value === "object";
}

export function isString(value) {
    return typeof value === "string";
}

export function getDefaultValueForAvroType(avroType) {
    let defaultValue;

    switch(avroType) {
        case avroTypes.BOOLEAN:
            defaultValue = false;
            break;
        case avroTypes.DOUBLE:
        case avroTypes.FLOAT:
        case avroTypes.INTEGER:
        case avroTypes.LONG:
            defaultValue = 0;
            break;
        case avroTypes.STRING:
            defaultValue = "";
            break;
        default:
            defaultValue = undefined;
    }

    return defaultValue;
}