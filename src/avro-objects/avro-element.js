
import {getDefaultValueForAvroType} from "../utilities";

export default class AvroElement {
    constructor(name, dataType, isNullable, defaultValue, parentNodes) {
        this._name = name;
        this._dataType = dataType;
        this._defaultValue = defaultValue;
        this._isNullable = isNullable;
        this._parentNodes = parentNodes;
        this._value = (isNullable) ? null : getDefaultValueForAvroType(dataType, isNullable);
    }

    get dataType() {
        return this._dataType;
    }

    get defaultValue() {
        return this._defaultValue;
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