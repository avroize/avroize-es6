import sanitize from "../avro-sanitizer";
import {isDefined, isObject} from "../utilities";
import avroTypes from "../constants/avro-types";

export default class JSONVisitor {
    visit(avroElement, data) {
        if (avroElement.dataType !== avroTypes.RECORD) {
            if (avroElement.parentNodes.length === 0) {
                avroElement._value = sanitize(avroElement.dataType, avroElement.isNullable,
                    avroElement.isArray, data);
            } else {
                // shift the root node and copy parents
                const parentNodes = avroElement.parentNodes.slice(1, avroElement.parentNodes.length);

                const currentObject = JSONVisitor.getCurrentObject(parentNodes, data);
                if (isObject(currentObject) && isDefined(currentObject[avroElement.name])) {
                    avroElement._value = sanitize(avroElement.dataType, avroElement.isNullable,
                        avroElement.isArray, currentObject[avroElement.name]);
                }
            }
        }
    }

    static getCurrentObject(parentNodes, data) {
        let currentObject, currentParent;

        if (parentNodes.length === 0) {
            currentObject = data;
        } else {
            currentParent = parentNodes.shift();

            if (isObject(data)) {
                currentObject = data[currentParent.name];
                if (currentParent.isNullable && isObject(currentObject)) {
                    currentObject = currentObject[currentParent.dataNodeName];
                }

                if (isObject(currentObject) && parentNodes.length > 0) {
                    currentObject = JSONVisitor.getCurrentObject(parentNodes, currentObject);
                }
            }
        }

        return currentObject;
    }
}