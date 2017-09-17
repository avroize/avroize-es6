
export default class AvroNode {
    constructor(name, dataNodeName, isNullable) {
        this._name = name;
        this._dataNodeName = dataNodeName;
        this._isNullable = isNullable;
    }

    get dataNodeName() {
        return this._dataNodeName;
    }

    get isNullable() {
        return this._isNullable;
    }

    get name() {
        return this._name;
    }
}
