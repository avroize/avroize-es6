import avroTypes from "./constants/avro-types";
import {getDefaultValueForAvroType, isArray as utilIsArray, isObject, isValidPrimitive} from "./utilities";

export default function sanitize(avroType, isNullable, isArray, value) {
    let result = getDefaultValueForAvroType(avroType, isNullable, isArray);

    if (isArray) {
        if (isNullable) {
            if (isObject(value) && utilIsArray(value[avroTypes.ARRAY])) {
                const santizedArray = value[avroTypes.ARRAY].filter((item) => {
                    return isValidPrimitive(avroType, item);
                });
                result = {};
                result[avroTypes.ARRAY] = santizedArray;
            }
        } else {
            if (utilIsArray(value)) {
                result = value.filter((item) => {
                    return isValidPrimitive(avroType, item);
                });
            }
        }
    } else if (isNullable) {
        if (isObject(value) && isValidPrimitive(avroType, value[avroType])) {
            result = value;
        }
    } else if (isValidPrimitive(avroType, value)) {
        result = value;
    }

    return result;
}