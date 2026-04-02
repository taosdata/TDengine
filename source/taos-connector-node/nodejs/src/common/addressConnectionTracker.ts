import { Address } from "./dsn";

export class AddressConnectionTracker {
    private static _instance: AddressConnectionTracker;
    private _counts: Map<string, number> = new Map();

    static instance(): AddressConnectionTracker {
        if (!AddressConnectionTracker._instance) {
            AddressConnectionTracker._instance = new AddressConnectionTracker();
        }
        return AddressConnectionTracker._instance;
    }

    increment(address: string): void {
        const current = this.getCount(address);
        this._counts.set(address, current + 1);
    }

    decrement(address: string): void {
        const current = this.getCount(address);
        if (current <= 1) {
            this._counts.delete(address);
            return;
        }
        this._counts.set(address, current - 1);
    }

    selectLeastConnected(addresses: Address[]): number {
        if (addresses.length === 0) {
            return 0;
        }

        let minCount = Number.MAX_SAFE_INTEGER;
        const candidateIndexes: number[] = [];

        for (let i = 0; i < addresses.length; i++) {
            const addr = addresses[i];
            const key = `${addr.host}:${addr.port}`;
            const count = this.getCount(key);

            if (count < minCount) {
                minCount = count;
                candidateIndexes.length = 0;
                candidateIndexes.push(i);
                continue;
            }

            if (count === minCount) {
                candidateIndexes.push(i);
            }
        }

        const selected = Math.floor(Math.random() * candidateIndexes.length);
        return candidateIndexes[selected];
    }

    getCount(address: string): number {
        return this._counts.get(address) ?? 0;
    }
}
