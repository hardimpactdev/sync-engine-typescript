// ULIDs for rows and changes made on the device, the same format as the server's
// sync_id: 26 upper-case Crockford characters.
// Ids made in the same millisecond count up, so they sort in creation order.

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const maxTime = 2 ** 48 - 1;

export function createUlidFactory(
    randomBytes: (count: number) => Uint8Array,
    clock: () => number = Date.now,
) {
    let lastTime = -1;
    let random: number[] = [];

    return function ulid(): string {
        let time = clock();
        if (!Number.isInteger(time) || time < 0 || time > maxTime) {
            throw new Error(`A ULID needs a millisecond time between 0 and 2^48, not ${time}.`);
        }
        if (time <= lastTime) {
            time = lastTime;
            increment(random);
        } else {
            lastTime = time;
            // 256 is a multiple of 32, so masking each byte keeps the digits uniform.
            random = Array.from(randomBytes(16), (byte) => byte & 31);
        }
        return encodeTime(time) + random.map((digit) => alphabet[digit]).join("");
    };
}

function encodeTime(time: number): string {
    let text = "";
    for (let index = 0; index < 10; index++) {
        text = alphabet[time % 32] + text;
        time = Math.floor(time / 32);
    }
    return text;
}

function increment(digits: number[]) {
    for (let index = digits.length - 1; index >= 0; index--) {
        if (digits[index] < 31) {
            digits[index]++;
            return;
        }
        digits[index] = 0;
    }
    throw new Error("Too many ULIDs in one millisecond.");
}
