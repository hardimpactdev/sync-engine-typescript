import type { SqlValue } from "./database";

// Counter arithmetic. Integers add as numbers. Decimals stored as text, such as
// "2.500", add exactly and keep the larger number of places.

export function addValues(total: SqlValue, delta: SqlValue): SqlValue {
    if (delta === null) return total;
    if (typeof total !== "string" && typeof delta !== "string") return (total ?? 0) + delta;
    const left = parse(total === null ? "0" : String(total));
    const right = parse(String(delta));
    const scale = Math.max(left.scale, right.scale);
    const sum = left.units * pow(scale - left.scale) + right.units * pow(scale - right.scale);
    return format(sum, scale);
}

export function negate(value: SqlValue): SqlValue {
    if (value === null) return null;
    if (typeof value === "number") return -value;
    const text = value.trim();
    return text.startsWith("-") ? text.slice(1) : `-${text}`;
}

function parse(text: string): { units: bigint; scale: number } {
    const trimmed = text.trim();
    if (!/^[-+]?(\d+\.?\d*|\.\d+)$/.test(trimmed)) throw new Error(`${text} is not a decimal.`);
    const negative = trimmed.startsWith("-");
    const [whole, fraction = ""] = trimmed.replace(/^[-+]/, "").split(".");
    const units = BigInt((whole || "0") + fraction);
    return { units: negative ? -units : units, scale: fraction.length };
}

function pow(exponent: number): bigint {
    return BigInt(10) ** BigInt(exponent);
}

function format(units: bigint, scale: number): string {
    const negative = units < BigInt(0);
    const digits = (negative ? -units : units).toString().padStart(scale + 1, "0");
    const text = scale === 0 ? digits : `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
    return negative ? `-${text}` : text;
}
