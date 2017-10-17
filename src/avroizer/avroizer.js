import _ from "lodash";
import AvroElement from "../avro-objects/avro-element";
import AvroNode from "../avro-objects/avro-node";
import avroTypes from "../constants/avro-types";
import {isArray, isDefined, isObject} from "../utilities";

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
        return this.avroizeElements(data, this.avroElements);
    }

    avroizeElements(data, avroElements) {
        const cleanAvroElements = _.cloneDeep(avroElements);

        let avroizedData = {};

        cleanAvroElements.forEach((avroElement) => {
            if (this.visitors !== null) {
                this.visitors.forEach((visitor) => {
                    avroElement.accept(visitor, data);
                });
            }

            const parentNodeLength = avroElement.parentNodes.length;
            if (parentNodeLength === 0) {
                avroizedData = avroElement.value;
            } else if (parentNodeLength === 1) {
                if (isDefined(avroElement.arrayElements)) {
                    avroizedData[avroElement.name] = this.getArrayRecords(avroElement, data);
                } else {
                    avroizedData[avroElement.name] = avroElement.value;
                }
            } else {
                let latestNode = avroizedData;
                let latestData = data;
                avroElement.parentNodes.forEach((parentNode) => {
                    // looking to get the right node object and data object
                    if (parentNode.dataNodeName !== null && latestData !== null) {
                        // get the right data first
                        if (isObject(latestData)) {
                            if (isObject(latestData[parentNode.name])) {
                                if (parentNode.isNullable) {
                                    latestData = latestData[parentNode.name][parentNode.dataNodeName];
                                } else {
                                    latestData = latestData[parentNode.name];
                                }
                            } else {
                                latestData = undefined;
                            }
                        }

                        // initialize the node here if not created yet
                        if (!isDefined(latestNode[parentNode.name])) {
                            if (parentNode.isNullable) {
                                if (isObject(latestData)) {
                                    latestNode[parentNode.name] = {};
                                    latestNode[parentNode.name][parentNode.dataNodeName] = {};
                                } else {
                                    latestNode[parentNode.name] = null;
                                    latestData = null;
                                }
                            } else {
                                latestNode[parentNode.name] = {};
                            }
                        }

                        // descend into the node if it exists
                        if (isObject(latestNode[parentNode.name])) {
                            if (parentNode.isNullable) {
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
                    if (isDefined(avroElement.arrayElements)) {
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

        if (isObject(data)) {
            if (avroElement.isNullable) {
                if (isObject(data[avroElement.name])) {
                    arrayElement = data[avroElement.name][avroTypes.ARRAY];
                }
            } else {
                arrayElement = data[avroElement.name];
            }
        }

        if (isArray(arrayElement)) {
            avroizedRecords = arrayElement.filter((record) => {
                return isObject(record);
            }).map((record) => {
                return this.avroizeElements(record, avroElement.arrayElements);
            });
        }

        if (avroElement.isNullable) {
            if (isArray(avroizedRecords)) {
                result = {};
                result[avroTypes.ARRAY] = avroizedRecords;
            } else {
                result = null;
            }
        } else {
            if (isArray(avroizedRecords)) {
                result = avroizedRecords;
            } else {
                result = [];
            }
        }

        return result;
    }

    static addAvroElementToAccumulator(accumulator, name, dataType, isNullable, isArray, defaultValue,
                                       parentNodes, arrayElements) {
        const avroElement = new AvroElement(name, dataType, isNullable, isArray, defaultValue,
            parentNodes, arrayElements);
        accumulator.push(avroElement);
    }

    static getAvroElement(avroJSON, parentNodes, accumulator, nodeName, namespace) {
        if (isObject(avroJSON.type)) {
            if (avroJSON.type.type === avroTypes.ARRAY) {
                if (isObject(avroJSON.type.items)) {
                    const arrayElements = Avroizer.getAvroElement(avroJSON.type.items, [], [], undefined, namespace);
                    Avroizer.addAvroElementToAccumulator(accumulator, avroJSON.name, avroJSON.type.type, false,
                        true, avroJSON.default, parentNodes, arrayElements);
                } else {
                    Avroizer.addAvroElementToAccumulator(accumulator, avroJSON.name, avroJSON.type.items, false,
                        true, avroJSON.default, parentNodes);
                }
            } else {
                return Avroizer.getAvroElement(avroJSON.type, parentNodes, accumulator, avroJSON.name, namespace);
            }
        } else if (avroJSON.type === avroTypes.RECORD) {
            let parentNode;

            if (isDefined(nodeName)) {
                const dataNodeName = namespace + "." + avroJSON.name;
                parentNode = new AvroNode(nodeName, dataNodeName, false);
            } else {
                if (!isDefined(namespace)) {
                    namespace = avroJSON.namespace;
                }
                parentNode = new AvroNode(avroJSON.name, null, false);
            }

            const newParentNodes = parentNodes.concat([parentNode]);
            avroJSON.fields.forEach((field) => {
                Avroizer.getAvroElement(field, newParentNodes, accumulator, undefined, namespace);
            });

            return accumulator;
        } else {
            if (isArray(avroJSON.type)) {
                const unionType = avroJSON.type[1];
                if (isObject(unionType)) {
                    if (unionType.type === avroTypes.ARRAY) {
                        if (isObject(unionType.items)) {
                            const arrayElements = Avroizer.getAvroElement(unionType.items, [], [], undefined, namespace);
                            Avroizer.addAvroElementToAccumulator(accumulator, avroJSON.name, unionType.type, true,
                                true, avroJSON.default, parentNodes, arrayElements);
                        } else {
                            Avroizer.addAvroElementToAccumulator(accumulator, avroJSON.name, unionType.items, true,
                                true, avroJSON.default, parentNodes);
                        }
                    } else {
                        const dataNodeName = namespace + "." + avroJSON.type[1].name;
                        const parentNode = new AvroNode(avroJSON.name, dataNodeName, true);

                        const newParentNodes = parentNodes.concat([parentNode]);
                        avroJSON.type[1].fields.forEach((field) => {
                            Avroizer.getAvroElement(field, newParentNodes, accumulator, undefined, namespace);
                        });

                        return accumulator;
                    }
                } else {
                    Avroizer.addAvroElementToAccumulator(accumulator, avroJSON.name, unionType, true, false,
                        avroJSON.default, parentNodes);
                }
            } else {
                Avroizer.addAvroElementToAccumulator(accumulator, avroJSON.name, avroJSON.type, false, false,
                    avroJSON.default, parentNodes);
            }

            return accumulator;
        }
    }
}
