
import avroTypes from "./constants/avro-types";

export function isDefined(value) {
    return typeof value !== "undefined";
}

export function isArray(value) {
    return Array.isArray(value);
}

export function isObject(value) {
    return value !== null && !this.isArray(value) && typeof value === "object";
}

export function isValidPrimitive(avroType, value) {
    let isValid;

    switch (avroType) {
        case avroTypes.BOOLEAN:
            isValid = typeof value === "boolean";
            break;
        case avroTypes.DOUBLE:
        case avroTypes.FLOAT:
            isValid = typeof value === "number";
            break;
        case avroTypes.INTEGER:
        case avroTypes.LONG:
            isValid = typeof value === "number" && Number.isInteger(value);
            break;
        case avroTypes.STRING:
            isValid = typeof value === "string";
            break;
        default:
            isValid = false;
    }

    return isValid;
}

export function getDefaultValueForAvroType(avroType, isNullable, isArray) {
    let defaultValue;

    if (isNullable) {
        defaultValue = null;
    } else if (isArray) {
        defaultValue = [];
    } else {
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
    }

    return defaultValue;
}