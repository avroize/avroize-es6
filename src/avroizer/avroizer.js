
import _ from "lodash";
import AvroElement from "../avro-objects/avro-element";
import AvroNode from "../avro-objects/avro-node";
import avroTypes from "../constants/avro-types";
import * as utilities from "../utilities";

export default class Avroizer {
    constructor(avroSchema, visitors) {
        this._avroElements = Avroizer.getAvroElement(avroSchema, [], []);
        this._visitors = visitors;
    }

    get avroElements() {
        return this._avroElements;
    }

    get visitors() {
        return this._visitors;
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

    static getAvroElement(avroJSON, parentNodes, accumulator, nodeName) {
        if (utilities.isObject(avroJSON.type)) {
            if (avroJSON.type.type === avroTypes.ARRAY) {
                // is an array, assume primitive type for now
                const avroElement = new AvroElement(avroJSON.name, avroJSON.type.items, false, true, avroJSON.default,
                    parentNodes);
                accumulator.push(avroElement);
                return accumulator;
            } else {
                // this is a record node with a data node
                return Avroizer.getAvroElement(avroJSON.type, parentNodes, accumulator, avroJSON.name);
            }
        } else if (avroJSON.type === avroTypes.RECORD) {
            let parentNode;
            if (utilities.isDefined(nodeName)) {
                parentNode = new AvroNode(nodeName, avroJSON.name, false);
            } else {
                parentNode = new AvroNode(avroJSON.name, null, false);
            }

            const newParentNodes = parentNodes.concat([parentNode]);
            avroJSON.fields.forEach((field) => {
                Avroizer.getAvroElement(field, newParentNodes, accumulator);
            });

            return accumulator;
        } else {
            let avroElement;

            if (utilities.isArray(avroJSON.type)) {
                // for MVP, assume that anything that is an array is a union with a nullable type
                // also assume first element is null, and the second element is the type
                // safety checks later :)

                const isArray = utilities.isObject(avroJSON.type[1]);
                const type = isArray ? avroJSON.type[1].items : avroJSON.type[1];
                avroElement = new AvroElement(avroJSON.name, type, true, isArray, avroJSON.default,
                    parentNodes);
            } else {
                avroElement = new AvroElement(avroJSON.name, avroJSON.type, false, false, avroJSON.default,
                    parentNodes);
            }

            accumulator.push(avroElement);
            return accumulator;
        }
    }
}
