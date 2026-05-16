/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
    preset: "ts-jest",
    transform: { "^.+\\.ts?$": "ts-jest" },
    testEnvironment: "node",
    testRegex: "/test/.*\\.(test|spec)?\\.(ts|tsx)$",
    moduleNameMapper: {
        "^@src/(.*)$": "<rootDir>/src/$1",
        "^@test-helpers/(.*)$": "<rootDir>/test/helpers/$1",
    },
    moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],
};
