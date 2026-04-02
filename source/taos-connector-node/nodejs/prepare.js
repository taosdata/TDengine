const fs = require("fs").promises;
const { readFile, writeFile, readdir, lstat } = fs;
const path = require("path");
const { resolve, parse } = path;

const tsFile = /\/src\/.*.ts$/;

async function dir(folder, ts = []) {
    let files = await readdir(folder);
    for (let f of files) {
        let path = resolve(folder, f).replace(/\\/g, "/"); // fix windows path
        let ls = await lstat(path);
        if (ls.isDirectory()) {
            await dir(path, ts);
        } else if (tsFile.test(path)) {
            let name = parse(path).name;
            if (!name.startsWith("_") && !name.endsWith("-back")) ts.push(path);
        }
    }
    return ts;
}

async function autoIndex() {
    let ts = await dir("./src");
    ts.sort(); // make sure same sort on windows and unix
    let improts = "",
        _dir = __dirname.replace(/\\/g, "/"); // fix windows path
    for (let path of ts) {
        improts += `export * from "${path
            .replace(_dir, ".")
            .slice(0, -3)}"\r\n`;
    }
    let content = await readFile(resolve(__dirname, "./index.ts"), "utf-8");
    if (improts !== content) {
        console.log("[autoIndex] index.ts");
        await writeFile(resolve(__dirname, "./index.ts"), improts);
    }
}

async function main() {
    await autoIndex();
}

main()
    .then((r) => console.log("done"))
    .catch((e) => console.error(e));
