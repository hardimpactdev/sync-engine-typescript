import { createContext, useCallback, useContext, useMemo, useSyncExternalStore } from "react";
import type { EngineTable, LocalStore } from "./store";

// React bindings, in their own entry so the store itself needs no React.

export const LocalStoreContext = createContext<LocalStore | null>(null);

export function useLocalStore<T extends string = string>(): LocalStore<T> {
    const store = useContext(LocalStoreContext);
    if (!store) throw new Error("useLocalStore needs a LocalStoreContext.");
    return store as LocalStore<T>;
}

/**
 * Reads local data and reads it again when one of the tables changes. Reads are
 * synchronous, so a page has its data on the first render and needs no spinner.
 */
export function useLocalQuery<R, T extends string = string>(
    tables: readonly (T | EngineTable)[],
    read: (store: LocalStore<T>) => R,
    deps: unknown[],
): R {
    const store = useLocalStore<T>();
    const key = tables.join(",");
    const subscribe = useCallback(
        (listener: () => void) => store.subscribe(tables, listener),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [store, key],
    );
    const version = useSyncExternalStore(subscribe, () => store.version(tables));
    // eslint-disable-next-line react-hooks/exhaustive-deps
    return useMemo(() => read(store), [store, version, ...deps]);
}
