// Compiles src/ to dist/, then points the declaration imports at the .js files
// next to them, so consumers resolve them whatever their module settings.
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";

rmSync("dist", { recursive: true, force: true });
execFileSync("npx", ["tsc", "-p", "tsconfig.build.json"], { stdio: "inherit" });

for (const file of readdirSync("dist")) {
    if (!file.endsWith(".d.ts")) continue;
    const path = `dist/${file}`;
    writeFileSync(path, readFileSync(path, "utf8").replace(/(from "\.\/[a-z-]+)\.ts"/g, '$1.js"'));
}
