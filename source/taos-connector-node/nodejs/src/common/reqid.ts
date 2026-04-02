import { createHash } from "crypto";
import { v4 as uuidv4 } from "uuid";
import { pid } from "node:process";

function hexStringToNumber(hexString: string): number {
    const number = parseInt(hexString, 16);
    if (isNaN(number)) {
        throw new Error(`number ${hexString} parse int fail!`);
    }
    return number;
}

function uuidToHash(): number {
    let uuid = uuidv4();
    // create SHA-256 hash
    const hash = createHash("sha256");

    // update hash contact
    hash.update(uuid);

    // get hex hash code
    const strHex = hash.digest("hex").substring(0, 8);

    let hex = hexStringToNumber(strHex);
    return hex & 0xff;
}

export class ReqId {
    private static _uuid = 0;
    private static _pid = 0;
    private static sharedBuffer = new SharedArrayBuffer(4);
    private static int32View = new Int32Array(ReqId.sharedBuffer);
    static {
        this._uuid = uuidToHash();

        if (pid) {
            this._pid = pid & 0xf;
        } else {
            this._pid = (Math.floor(Math.random() * 9000) + 1000) & 0xf;
        }

        Atomics.store(ReqId.int32View, 0, 0);
    }

    public static getReqID(req_id?: number): number {
        if (req_id) {
            return req_id;
        }
        let no = Atomics.add(ReqId.int32View, 0, 1);
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        let ts = new Date().getTime() >> 8;
        view.setUint8(6, this._uuid >> 4);
        view.setUint8(5, ((this._uuid & 0x0f) << 4) | this._pid);
        view.setUint8(4, (ts >> 16) & 0xff);
        view.setUint16(2, ts & 0xffff, true);
        view.setUint16(0, no & 0xffff, true);
        let id = view.getBigInt64(0, true);
        return Number(id);
    }
}
