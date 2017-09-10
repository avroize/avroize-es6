
import exampleTest from "../src/index";

describe("index", () => {
    describe("exampleTest", () => {
        test("this is just a test result", () => {
            const result = exampleTest();

            expect(result).toEqual("This is just a test.");
        });
    });
});