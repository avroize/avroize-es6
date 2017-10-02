
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
            if (this.visitors !== null) {
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
                let latestData = data;
                avroElement.parentNodes.forEach((parentNode) => {
                    if (parentNode.dataNodeName !== null && latestData !== null) {
                        if (utilities.isDefined(latestData)) {
                            if (utilities.isObject(latestData[parentNode.name]) || latestData[parentNode.name] === null) {
                                latestData = latestData[parentNode.name];
                                if (parentNode.isNullable && latestData !== null) {
                                    latestData = latestData[parentNode.dataNodeName];
                                }
                            } else {
                                latestData = undefined;
                            }
                        }

                        if (!utilities.isDefined(latestNode[parentNode.name])) {
                            if (parentNode.isNullable && (latestData === null || !utilities.isObject(latestData))) {
                                // only nullable nodes can be nulled out
                                // or nullable nodes that dont have data defined for them (null by default)
                                latestNode[parentNode.name] = null;
                                latestData = null;
                            } else {
                                latestNode[parentNode.name] = {};
                            }
                        }

                        if (latestNode[parentNode.name] !== null) {
                            if (parentNode.isNullable) {
                                if (!utilities.isDefined(latestNode[parentNode.name][parentNode.dataNodeName])) {
                                    latestNode[parentNode.name][parentNode.dataNodeName] = {};
                                }
                                latestNode = latestNode[parentNode.name][parentNode.dataNodeName];
                            } else {
                                latestNode = latestNode[parentNode.name];
                            }
                        } else {
                            latestData = null;
                        }
                    }
                });

                if (latestData !== null) {
                    latestNode[avroElement.name] = avroElement.value;
                }
            }
        });

        return avroizedData;
    }

    static getAvroElement(avroJSON, parentNodes, accumulator, nodeName, namespace) {
        let avroElement, newParentNodes, parentNode;

        if (utilities.isObject(avroJSON.type)) {
            if (avroJSON.type.type === avroTypes.ARRAY) {
                // is an array, assume primitive type for now
                avroElement = new AvroElement(avroJSON.name, avroJSON.type.items, false, true, avroJSON.default,
                    parentNodes);
                accumulator.push(avroElement);
                return accumulator;
            } else {
                // this is a record node with a non-nullable data node
                return Avroizer.getAvroElement(avroJSON.type, parentNodes, accumulator, avroJSON.name, namespace);
            }
        } else if (avroJSON.type === avroTypes.RECORD) {
            if (utilities.isDefined(nodeName)) {
                const dataNodeName = namespace + "." + avroJSON.name;
                parentNode = new AvroNode(nodeName, dataNodeName, false);
            } else {
                // this is the root node, grab the namespace from it
                parentNode = new AvroNode(avroJSON.name, null, false);
                namespace = avroJSON.namespace;
            }

            newParentNodes = parentNodes.concat([parentNode]);
            avroJSON.fields.forEach((field) => {
                Avroizer.getAvroElement(field, newParentNodes, accumulator, undefined, namespace);
            });

            return accumulator;
        } else {
            if (utilities.isArray(avroJSON.type)) {
                // for MVP, assume that anything that is an array is a union with a nullable type
                // also assume first element is null, and the second element is the type
                // safety checks later :)

                const unionType = avroJSON.type[1];
                if (utilities.isObject(unionType)) {
                    if (unionType.type === avroTypes.ARRAY) {
                        avroElement = new AvroElement(avroJSON.name, unionType.items, true, true,
                            avroJSON.default, parentNodes);
                    } else {
                        // this is a nullable data node
                        const dataNodeName = namespace + "." + avroJSON.type[1].name;
                        parentNode = new AvroNode(avroJSON.name, dataNodeName, true);

                        newParentNodes = parentNodes.concat([parentNode]);
                        avroJSON.type[1].fields.forEach((field) => {
                            Avroizer.getAvroElement(field, newParentNodes, accumulator, undefined, namespace);
                        });

                        return accumulator;
                    }
                } else {
                    avroElement = new AvroElement(avroJSON.name, unionType, true, false,
                        avroJSON.default, parentNodes);
                }
            } else {
                avroElement = new AvroElement(avroJSON.name, avroJSON.type, false, false, avroJSON.default,
                    parentNodes);
            }

            accumulator.push(avroElement);
            return accumulator;
        }
    }
}
