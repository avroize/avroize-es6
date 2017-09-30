
import avroTypes from "./constants/avro-types";
import * as utilities from "./utilities";

export default function sanitize(type, isNullable, value) {
    let result;

    switch (type) {
        case avroTypes.BOOLEAN:
            result = sanitizeBoolean(value, isNullable);
            break;
        case avroTypes.DOUBLE:
            result = sanitizeDouble(value, isNullable);
            break;
        case avroTypes.FLOAT:
            result = sanitizeFloat(value, isNullable);
            break;
        case avroTypes.INTEGER:
            result = sanitizeInteger(value, isNullable);
            break;
        case avroTypes.LONG:
            result = sanitizeLong(value, isNullable);
            break;
        case avroTypes.STRING:
            result = sanitizeString(value, isNullable);
            break;
    }

    return result;
}

function sanitizeBoolean(value, isNullable) {
    let result;

    if (isNullable) {
        if (utilities.isObject(value) && utilities.isBoolean(value[avroTypes.BOOLEAN])) {
            result = value;
        } else {
            result = null;
        }
    } else if (utilities.isBoolean(value)) {
        result = value;
    } else {
        result = utilities.getDefaultValueForAvroType(avroTypes.BOOLEAN);
    }

    return result;
}

function sanitizeDouble(value, isNullable) {
    let result;

    if (isNullable) {
        if (utilities.isObject(value) && utilities.isDouble(value[avroTypes.DOUBLE])) {
            result = value;
        } else {
            result = null;
        }
    } else if (utilities.isDouble(value)) {
        result = value;
    } else {
        result = utilities.getDefaultValueForAvroType(avroTypes.DOUBLE);
    }

    return result;
}

function sanitizeFloat(value, isNullable) {
    let result;

    if (isNullable) {
        if (utilities.isObject(value) && utilities.isFloat(value[avroTypes.FLOAT])) {
            result = value;
        } else {
            result = null;
        }
    } else if (utilities.isFloat(value)) {
        result = value;
    } else {
        result = utilities.getDefaultValueForAvroType(avroTypes.FLOAT);
    }

    return result;
}

function sanitizeInteger(value, isNullable) {
    let result;

    if (isNullable) {
        if (utilities.isObject(value) && utilities.isInteger(value[avroTypes.INTEGER])) {
            result = value;
        } else {
            result = null;
        }
    } else if (utilities.isInteger(value)) {
        result = value;
    } else {
        result = utilities.getDefaultValueForAvroType(avroTypes.INTEGER);
    }

    return result;
}

function sanitizeLong(value, isNullable) {
    let result;

    if (isNullable) {
        if (utilities.isObject(value) && utilities.isLong(value[avroTypes.LONG])) {
            result = value;
        } else {
            result = null;
        }
    } else if (utilities.isLong(value)) {
        result = value;
    } else {
        result = utilities.getDefaultValueForAvroType(avroTypes.LONG);
    }

    return result;
}

function sanitizeString(value, isNullable) {
    let result;

    if (isNullable) {
        if (utilities.isObject(value) && utilities.isString(value[avroTypes.STRING])) {
            result = value;
        } else {
            result = null;
        }
    } else if (utilities.isString(value)) {
        result = value;
    } else {
        result = utilities.getDefaultValueForAvroType(avroTypes.STRING);
    }

    return result;
}