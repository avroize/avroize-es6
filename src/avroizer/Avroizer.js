
import _ from "lodash";
import AvroElement from "../avro-objects/AvroElement";
import AvroNode from "../avro-objects/AvroNode";
import * as utilities from "../utilities";

export default class Avroizer {
    constructor(avroSchema, visitors) {
        this._avroElements = Avroizer.createAvroElement(avroSchema, [], []);
        this.visitors = visitors;
    }

    get avroElements() {
        return this._avroElements;
    }

    avroize(data) {
        // data is generic, could be any data type
        // the type of the visitor will correlate with the data type
        // other visitors can determine the default behavior (e.g. use the default values or null types)
        // iterate avroElements, accepting all the registered visitors for each

        // clone avro elements to get a clean default tree
        const cleanAvroElements = _.cloneDeep(this.avroElements);

        let avroizedData = {};

        cleanAvroElements.forEach((avroElement) => {
            if (utilities.isDefined(this.visitors)) {
                // each visitor will be applied in order
                this.visitors.forEach((visitor) => {
                    avroElement.accept(visitor, data);
                });
            }

            const parentNodeLength = avroElement.parentNodes.length;
            if (parentNodeLength === 0) {
                // this is a schema with a primitive root
                avroizedData = avroElement.value;
            } else if (parentNodeLength === 1) {
                // this is a schema with an object root
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
            const avroElement = new AvroElement(avroJSON.name, avroJSON.type, avroJSON.default, parentNodes);
            accumulator.push(avroElement);
            return accumulator;
        }
    }
}
