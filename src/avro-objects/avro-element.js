
import {getDefaultValueForAvroType} from "../utilities";

export default class AvroElement {
    constructor(name, dataType, defaultValue, parentNodes) {
        this._name = name;
        this._dataType = dataType;
        this._defaultValue = defaultValue;
        this._parentNodes = parentNodes;
        this._value = getDefaultValueForAvroType(dataType);
    }

    get dataType() {
        return this._dataType;
    }

    get defaultValue() {
        return this._defaultValue;
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