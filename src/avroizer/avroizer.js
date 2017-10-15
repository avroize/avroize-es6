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
                    if (parentNode.dataNodeName !== null && latestData !== null) {
                        if (isDefined(latestData)) {
                            if (isObject(latestData[parentNode.name]) || latestData[parentNode.name] === null) {
                                latestData = latestData[parentNode.name];
                                if (parentNode.isNullable && latestData !== null) {
                                    latestData = latestData[parentNode.dataNodeName];
                                }
                            } else {
                                latestData = undefined;
                            }
                        }

                        if (!isDefined(latestNode[parentNode.name])) {
                            if (parentNode.isNullable && (latestData === null || !isObject(latestData))) {
                                latestNode[parentNode.name] = null;
                                latestData = null;
                            } else {
                                latestNode[parentNode.name] = {};
                            }
                        }

                        if (latestNode[parentNode.name] !== null) {
                            if (parentNode.isNullable) {
                                if (!isDefined(latestNode[parentNode.name][parentNode.dataNodeName])) {
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

        if (isDefined(data)) {
            if (avroElement.isNullable) {
                if (isDefined(data[avroElement.name])) {
                    arrayElement = data[avroElement.name][avroTypes.ARRAY];
                }
            } else {
                arrayElement = data[avroElement.name];
            }
        }

        if (isArray(arrayElement)) {
            avroizedRecords = [];

            const recordElements = arrayElement.filter((record) => {
                return isObject(record);
            });

            recordElements.forEach((record) => {
                const avroizedRecord = this.avroizeElements(record, avroElement.arrayElements);
                avroizedRecords.push(avroizedRecord);
            });
        }

        if (avroElement.isNullable) {
            if (isDefined(avroizedRecords)) {
                result = {};
                result[avroTypes.ARRAY] = avroizedRecords;
            } else {
                result = null;
            }
        } else {
            if (isDefined(avroizedRecords)) {
                result = avroizedRecords;
            } else {
                result = [];
            }
        }

        return result;
    }

    static getAvroElement(avroJSON, parentNodes, accumulator, nodeName, namespace) {
        let avroElement, newParentNodes, parentNode;

        if (isObject(avroJSON.type)) {
            if (avroJSON.type.type === avroTypes.ARRAY) {
                if (isObject(avroJSON.type.items)) {
                    const arrayElements = Avroizer.getAvroElement(avroJSON.type.items, [], [], undefined, namespace);
                    avroElement = new AvroElement(avroJSON.name, avroJSON.type.type, false, true, avroJSON.default,
                        parentNodes, arrayElements);
                    accumulator.push(avroElement);
                    return accumulator;
                } else {
                    avroElement = new AvroElement(avroJSON.name, avroJSON.type.items, false, true, avroJSON.default,
                        parentNodes);
                    accumulator.push(avroElement);
                    return accumulator;
                }
            } else {
                return Avroizer.getAvroElement(avroJSON.type, parentNodes, accumulator, avroJSON.name, namespace);
            }
        } else if (avroJSON.type === avroTypes.RECORD) {
            if (isDefined(nodeName)) {
                const dataNodeName = namespace + "." + avroJSON.name;
                parentNode = new AvroNode(nodeName, dataNodeName, false);
            } else {
                if (!isDefined(namespace)) {
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
            if (isArray(avroJSON.type)) {
                const unionType = avroJSON.type[1];
                if (isObject(unionType)) {
                    if (unionType.type === avroTypes.ARRAY) {
                        if (isObject(unionType.items)) {
                            const arrayElements = Avroizer.getAvroElement(unionType.items, [], [], undefined, namespace);
                            avroElement = new AvroElement(avroJSON.name, unionType.type, true, true, avroJSON.default,
                                parentNodes, arrayElements);
                        } else {
                            avroElement = new AvroElement(avroJSON.name, unionType.items, true, true,
                                avroJSON.default, parentNodes);
                        }
                    } else {
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
