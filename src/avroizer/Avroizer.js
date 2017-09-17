
import AvroElement from "../avro-objects/AvroElement";
import AvroNode from "../avro-objects/AvroNode";
import * as utilities from "../utilities";

export default class Avroizer {
    constructor(avroSchema, visitors) {
        this.avroElements = Avroizer.createAvroElement(avroSchema, [], []);
        this.visitors = visitors;
    }

    avroize(data) {
        // data is generic, could be any data type
        // the type of the visitor will correlate with the data type
        // other visitors can determine the default behavior (e.g. use the default values or null types)
        // iterate avroElements, accepting all the registered visitors for each

        const avroizedData = {};

        this.avroElements.forEach((avroElement) => {
            const parentNodeLength = avroElement.parentNodes.length;
            if (parentNodeLength === 0 || parentNodeLength === 1) {
                // this is the root node
                avroizedData[avroElement.name] = avroElement.value;
            } else {
                let latestNode = avroizedData;
                avroElement.parentNodes.forEach((parentNode) => {
                    if (parentNode.dataNodeName !== null) {
                        if (!utilities.isDefined(latestNode[parentNode.name])) {
                            latestNode[parentNode.name] = {};
                        }
                        latestNode = latestNode[parentNode.name];
                    }
                });
                latestNode[avroElement.name] = avroElement.value;
            }
        });

        return avroizedData;
    }

    static createAvroElement(avroJSON, parentNodes, accumulator, nodeName) {
        if (utilities.isObject(avroJSON.type)) {
            // this is a record node with a data node
            return Avroizer.createAvroElement(avroJSON.type, parentNodes, accumulator, avroJSON.name);
        } else if (avroJSON.type === "record") {
            let parentNode;
            if (utilities.isDefined(nodeName)) {
                parentNode = new AvroNode(nodeName, avroJSON.name, false);
            } else {
                parentNode = new AvroNode(avroJSON.name, null, false);
            }

            const newParentNodes = parentNodes.concat([parentNode]);
            avroJSON.fields.forEach((field) => {
                Avroizer.createAvroElement(field, newParentNodes, accumulator);
            });

            return accumulator;
        } else {
            const avroElement = new AvroElement(avroJSON.name, avroJSON.type,
                avroJSON.default, parentNodes);
            accumulator.push(avroElement);
            return accumulator;
        }
    }
}
