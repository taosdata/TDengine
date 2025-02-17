// Numbers fundamental to the encoding.
// the "error" Rune or "Unicode replacement character"
const RuneError = '\uFFFD'
// Maximum valid Unicode code point.
const MaxRune = '\U0010FFFF'
// Code points in the surrogate range are not valid for UTF-8.
const surrogateMin = 0xD800
const surrogateMax = 0xDFFF
const tx = 128;
const t2 = 192;
const t3 = 224;
const t4 = 240;
const maskx = 63;
const rune1Max = (1 << 7) - 1;

const rune2Max = (1 << 11) - 1;
const rune3Max = (1 << 16) - 1;


// AppendRune appends the UTF-8 encoding of r to the end of p and
// returns the extended buffer. If the rune is out of range,
// it appends the encoding of RuneError.
export function AppendRune(r:any) {
    let p:Array<any> = [];
    // console.log("== AppendRun r:");
    // console.log(r)
    if (r <= rune1Max) {
        p.push(r & 0xff);  
        return Buffer.from(p).toString();
    }
    if (r <= rune2Max) {
        p.push(t2 | ((r >> 6) & 0xff), tx | (r & 0xff) & maskx)
    } else if ((r > MaxRune) || (surrogateMax <= r && r <= surrogateMax)) {
        p.push(RuneError)
    } else if (r <= rune3Max) {
        p.push(t3 | ((r >> 12) & 0xff), tx | ((r >> 6) & 0xff) & maskx, tx | (r & 0xff) & maskx)
    } else {
        p.push(t4 | ((r >> 18) & 0xff), tx | ((r >> 12) & 0xff) & maskx, tx | ((r >> 6) & 0xff) & maskx, tx | (r & 0xff) & maskx)
    }

    return Buffer.from(p).toString();
}