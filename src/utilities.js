
function isDefined(value) {
    return typeof value !== "undefined";
}

function isObject(value) {
    return value !== null && typeof value === "object";
}

export {isDefined, isObject};