
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

        return this.avroizeElements(data, this.avroElements);
    }

    avroizeElements(data, avroElements) {

        // clone avro elements to get a clean default tree
        const cleanAvroElements = _.cloneDeep(avroElements);

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
                if (utilities.isDefined(avroElement.arrayElements)) {
                    avroizedData[avroElement.name] = this.getArrayRecords(avroElement, data);
                } else {
                    avroizedData[avroElement.name] = avroElement.value;
                }
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
                                // need a check here to inspect array data!
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
                    if (utilities.isDefined(avroElement.arrayElements)) {
                        latestNode[avroElement.name] = this.getArrayRecords(avroElement, latestData);
                    } else {
                        latestNode[avroElement.name] = avroElement.value;
                    }
                }
            }
        });

        return avroizedData;
    }

    getArrayRecords(avroElement, data) {
        let arrayElement, avroizedRecords, result;

        if (utilities.isDefined(data)) {
            if (avroElement.isNullable) {
                if (utilities.isDefined(data[avroElement.name])) {
                    arrayElement = data[avroElement.name][avroTypes.ARRAY];
                }
            } else {
                arrayElement = data[avroElement.name];
            }
        }

        if (utilities.isArray(arrayElement)) {
            avroizedRecords = [];

            const recordElements = arrayElement.filter((record) => {
                return utilities.isObject(record);
            });

            recordElements.forEach((record) => {
                const avroizedRecord = this.avroizeElements(record, avroElement.arrayElements);
                avroizedRecords.push(avroizedRecord);
            });
        }

        if (avroElement.isNullable) {
            if (utilities.isDefined(avroizedRecords)) {
                result = {};
                result[avroTypes.ARRAY] = avroizedRecords;
            } else {
                result = null;
            }
        } else {
            if (utilities.isDefined(avroizedRecords)) {
                result = avroizedRecords;
            } else {
                result = [];
            }
        }

        return result;
    }

    static getAvroElement(avroJSON, parentNodes, accumulator, nodeName, namespace) {
        let avroElement, newParentNodes, parentNode;

        if (utilities.isObject(avroJSON.type)) {
            if (avroJSON.type.type === avroTypes.ARRAY) {
                // non nullable array
                if (utilities.isObject(avroJSON.type.items)) {
                    // record array
                    const arrayElements = Avroizer.getAvroElement(avroJSON.type.items, [], [], undefined, namespace);
                    avroElement = new AvroElement(avroJSON.name, avroJSON.type.type, false, true, avroJSON.default,
                        parentNodes, arrayElements);
                    accumulator.push(avroElement);
                    return accumulator;
                } else {
                    // primitive array
                    avroElement = new AvroElement(avroJSON.name, avroJSON.type.items, false, true, avroJSON.default,
                        parentNodes);
                    accumulator.push(avroElement);
                    return accumulator;
                }
            } else {
                // this is a record node with a non-nullable data node
                return Avroizer.getAvroElement(avroJSON.type, parentNodes, accumulator, avroJSON.name, namespace);
            }
        } else if (avroJSON.type === avroTypes.RECORD) {
            if (utilities.isDefined(nodeName)) {
                const dataNodeName = namespace + "." + avroJSON.name;
                parentNode = new AvroNode(nodeName, dataNodeName, false);
            } else {
                if (!utilities.isDefined(namespace)) {
                    // this is the root node, grab the namespace from it
                    namespace = avroJSON.namespace;
                }
                parentNode = new AvroNode(avroJSON.name, null, false);
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
                        // a nullable array
                        if (utilities.isObject(unionType.items)) {
                            const arrayElements = Avroizer.getAvroElement(unionType.items, [], [], undefined, namespace);
                            avroElement = new AvroElement(avroJSON.name, unionType.type, true, true, avroJSON.default,
                                parentNodes, arrayElements);
                        } else {
                            avroElement = new AvroElement(avroJSON.name, unionType.items, true, true,
                                avroJSON.default, parentNodes);
                        }
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
