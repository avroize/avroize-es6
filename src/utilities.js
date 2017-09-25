
import {avroTypes} from "./constants/avro-types";

function isDefined(value) {
    return typeof value !== "undefined" && value !== null;
}

function isInteger(value) {
    return typeof value === "number" && Number.isInteger(value);
}

function isObject(value) {
    return value !== null && typeof value === "object";
}

function isString(value) {
    return typeof value === "string";
}

function getDefaultValueForAvroType(avroType) {
    let defaultValue;

    switch(avroType) {
        case avroTypes.STRING:
            defaultValue = "";
            break;
        case avroTypes.INTEGER:
            defaultValue = 0;
            break;
        default:
            defaultValue = undefined;
    }

    return defaultValue;
}

export {isDefined, isInteger, isObject, isString, getDefaultValueForAvroType};