
import {isDefined, isObject} from "../src/utilities";

let result;

describe("utilities", () => {
    describe("isDefined", () => {
        test("return false if value is undefined", () => {
            result = isDefined(undefined);

            expect(result).toBeFalsy();
        });

        test("return false if value is null", () => {
            result = isDefined(null);

            expect(result).toBeFalsy();
        });

        test("return true if value is otherwise", () => {
            result = isDefined("value");

            expect(result).toBeTruthy();
        });
    });
});