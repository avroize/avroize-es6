
import {avroTypes} from "../constants/AvroTypes";

export default class AvroElement {
    constructor(name, dataType, defaultValue, parentNodes) {
        this._name = name;
        this._dataType = dataType;
        this._defaultValue = defaultValue;
        this._parentNodes = parentNodes;
        this._value = AvroElement.getDefaultValueForAvroType(dataType);
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

    accept(visitor) {
        visitor.visit(this);
    }

    static getDefaultValueForAvroType(avroType) {
        let defaultValue;

        switch(avroType) {
            case avroTypes.STRING:
                defaultValue = "";
                break;
            case avroTypes.INTEGER:
                defaultValue = 0;
                break;
            default:
                defaultValue = undefined;
        }

        return defaultValue;
    }
}