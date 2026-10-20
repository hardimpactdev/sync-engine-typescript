import assert from "node:assert/strict";
import test from "node:test";
import { addValues, negate } from "../src/decimal.ts";

test("counters add exactly, as numbers or as decimal text", () => {
    assert.equal(addValues(2, 1), 3);
    assert.equal(addValues(null, -1), -1);
    assert.equal(addValues("1.000", "0.250"), "1.250");
    assert.equal(addValues("0.1", "0.2"), "0.3");
    assert.equal(addValues("1.500", "-2.000"), "-0.500");
    assert.equal(addValues("2", 1), "3");
    assert.equal(addValues("2.000", null), "2.000");
    assert.equal(negate("-1.500"), "1.500");
    assert.equal(negate("0.250"), "-0.250");
    assert.throws(() => addValues("1,5", "1"), /not a decimal/);
});
