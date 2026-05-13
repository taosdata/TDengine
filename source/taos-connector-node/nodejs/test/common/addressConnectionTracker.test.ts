import { AddressConnectionTracker } from "@src/common/addressConnectionTracker";
import { Address } from "@src/common/dsn";

function resetTracker(): AddressConnectionTracker {
    const tracker = AddressConnectionTracker.instance();
    (tracker as any)._counts.clear();
    return tracker;
}

describe("AddressConnectionTracker", () => {
    beforeEach(() => {
        resetTracker();
    });

    afterEach(() => {
        jest.restoreAllMocks();
        resetTracker();
    });

    test("increment/decrement updates counts correctly", () => {
        const tracker = resetTracker();

        tracker.increment("host1:6041");
        tracker.increment("host1:6041");
        tracker.increment("host2:6042");

        expect(tracker.getCount("host1:6041")).toBe(2);
        expect(tracker.getCount("host2:6042")).toBe(1);

        tracker.decrement("host1:6041");
        expect(tracker.getCount("host1:6041")).toBe(1);
    });

    test("decrement never makes count negative", () => {
        const tracker = resetTracker();

        tracker.decrement("host1:6041");
        tracker.increment("host1:6041");
        tracker.decrement("host1:6041");
        tracker.decrement("host1:6041");

        expect(tracker.getCount("host1:6041")).toBe(0);
    });

    test("selectLeastConnected returns index with minimum count", () => {
        const tracker = resetTracker();
        const addresses = [
            new Address("host1", 6041),
            new Address("host2", 6042),
            new Address("host3", 6043),
        ];

        tracker.increment("host1:6041");
        tracker.increment("host1:6041");
        tracker.increment("host3:6043");

        const index = tracker.selectLeastConnected(addresses);
        expect(index).toBe(1);
    });

    test("selectLeastConnected uses random tie-break among least-connected addresses", () => {
        const tracker = resetTracker();
        const addresses = [
            new Address("host1", 6041),
            new Address("host2", 6042),
            new Address("host3", 6043),
        ];

        tracker.increment("host3:6043");
        tracker.increment("host3:6043");

        const rounds = 400;
        let host1Count = 0;
        let host2Count = 0;

        for (let i = 0; i < rounds; i++) {
            const selected = tracker.selectLeastConnected(addresses);
            if (selected === 0) {
                host1Count += 1;
            }
            if (selected === 1) {
                host2Count += 1;
            }
            expect(selected).not.toBe(2);
        }

        expect(host1Count).toBeGreaterThan(80);
        expect(host2Count).toBeGreaterThan(80);
    });
});
