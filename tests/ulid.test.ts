import assert from "node:assert/strict";
import test from "node:test";
import { createUlidFactory } from "../src/ulid.ts";

test("ULIDs are 26 upper-case Crockford characters that sort in creation order", () => {
    let time = 1_760_000_000_000;
    const ulid = createUlidFactory(
        (count) => new Uint8Array(count).fill(7),
        () => time,
    );
    const first = ulid();
    assert.match(first, /^[0-9A-HJKMNP-TV-Z]{26}$/);
    const sameMillisecond = ulid();
    time -= 5; // a clock that steps back still yields a later id
    const clockBack = ulid();
    time += 10;
    const later = ulid();
    assert.deepEqual([first, sameMillisecond, clockBack, later].sort(), [
        first,
        sameMillisecond,
        clockBack,
        later,
    ]);
    assert.equal(new Set([first, sameMillisecond, clockBack, later]).size, 4);
});

test("a ULID needs a time that fits in 48 bits", () => {
    const ulid = createUlidFactory(
        (count) => new Uint8Array(count),
        () => -1,
    );
    assert.throws(() => ulid(), /millisecond time/);
});
