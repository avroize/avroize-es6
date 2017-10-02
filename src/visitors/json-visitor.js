
import sanitize from "../avro-sanitizer";
import * as utilities from "../utilities";
import avroTypes from "../constants/avro-types";

export default class JSONVisitor {
    visit(avroElement, data) {
        if (avroElement.dataType !== avroTypes.RECORD) {
            if (avroElement.parentNodes.length === 0) {
                // this is a single field schema
                avroElement._value = sanitize(avroElement.dataType, avroElement.isNullable,
                    avroElement.isArray, data);
            } else {
                // shift the root node and copy parents
                const parentNodes = avroElement.parentNodes.slice(1, avroElement.parentNodes.length);

                const currentObject = JSONVisitor.getCurrentObject(parentNodes, data);
                if (utilities.isObject(currentObject) && utilities.isDefined(currentObject[avroElement.name])) {
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

            if (utilities.isObject(data)) {
                currentObject = data[currentParent.name];
                if (currentParent.isNullable && utilities.isObject(currentObject)) {
                    currentObject = currentObject[currentParent.dataNodeName];
                }

                if (utilities.isObject(currentObject) && parentNodes.length > 0) {
                    // keep looking for deepest node
                    currentObject = JSONVisitor.getCurrentObject(parentNodes, currentObject);
                }
            }
        }

        return currentObject;
    }
}