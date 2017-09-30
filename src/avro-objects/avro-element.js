
import {getDefaultValueForAvroType} from "../utilities";

export default class AvroElement {
    constructor(name, dataType, isNullable, isArray, defaultValue, parentNodes) {
        this._name = name;
        this._dataType = dataType;
        this._defaultValue = defaultValue;
        this._isArray = isArray;
        this._isNullable = isNullable;
        this._parentNodes = parentNodes;
        this._value = getDefaultValueForAvroType(dataType, isNullable, isArray);
    }

    get dataType() {
        return this._dataType;
    }

    get defaultValue() {
        return this._defaultValue;
    }

    get isArray() {
        return this._isArray;
    }

    get isNullable() {
        return this._isNullable;
    }

    get name() {
        return this._name;
    }

    get parentNodes() {
        return this._parentNodes;
    }

    get value() {
        return this._value;
    }

    accept(visitor, data) {
        visitor.visit(this, data);
    }
}