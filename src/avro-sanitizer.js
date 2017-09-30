
import avroTypes from "./constants/avro-types";
import * as utilities from "./utilities";

export default function sanitize(avroType, isNullable, isArray, value) {
    let result = utilities.getDefaultValueForAvroType(avroType, isNullable, isArray);

    if (isArray) {
        if (isNullable) {
            if (utilities.isObject(value) && utilities.isArray(value[avroTypes.ARRAY])) {
                const santizedArray = value[avroTypes.ARRAY].filter((item) => {
                    return utilities.isValidPrimitive(avroType, item);
                });
                result = {};
                result[avroTypes.ARRAY] = santizedArray;
            }
        } else {
            if (utilities.isArray(value)) {
                result = value.filter((item) => {
                    return utilities.isValidPrimitive(avroType, item);
                });
            }
        }
    } else if (isNullable) {
        if (utilities.isObject(value) && utilities.isValidPrimitive(avroType, value[avroType])) {
            result = value;
        }
    } else if (utilities.isValidPrimitive(avroType, value)) {
        result = value;
    }

    return result;
}