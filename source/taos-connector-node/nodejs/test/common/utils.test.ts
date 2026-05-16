import { compareVersions } from "@src/common/utils";

describe("utils test", () => {
    test("compare versions test", async () => {
        expect(compareVersions("3.3.6.3-alpha", "3.3.6.2")).toBe(1);
        expect(compareVersions("3.3.6.2", "3.3.6.3")).toBe(-1);
        expect(compareVersions("3.3.6.3", "3.3.6.3")).toBe(0);
        expect(compareVersions("3.3.6.3", "3.3.6.3-alpha")).toBe(1);
        expect(compareVersions("3.3.6.3-beta", "3.3.6.3-alpha")).toBe(1);
        expect(compareVersions("3.3", "3.3.0.0")).toBe(0);
    });
});
