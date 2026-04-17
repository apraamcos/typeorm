import { AsyncLocalStorage } from "async_hooks"

/**
 * Per-call retry deadline propagated across the async call stack so that
 * multiple layers of retry logic share a single, non-stacking budget.
 *
 * Without this, an outer layer's `maxRetryDuration` would stack with each
 * inner layer's budget (e.g. EntityManager.transaction's 5 min + query's
 * 5 min + obtainMasterConnection's 5 min = up to 15 min in the worst case).
 * By binding a deadline at the outermost retry entry point and having
 * inner layers inherit it, the total retry window is capped at the value
 * the outermost caller intended.
 *
 * Semantics:
 *   - An absolute millisecond timestamp (Date.now() + budget).
 *   - Inner callers read the current deadline via `getRetryDeadline()`
 *     and only establish a new one via `withRetryDeadline()` when none is
 *     already active.
 *   - AsyncLocalStorage ensures correct isolation between concurrent
 *     unrelated callers (e.g. two independent DataSource.transaction()
 *     invocations in parallel).
 */
const store = new AsyncLocalStorage<{ deadline: number }>()

export function getRetryDeadline(): number | undefined {
    return store.getStore()?.deadline
}

/**
 * Run `fn` with a shared retry deadline. If a deadline is already active
 * (we are nested inside another retry boundary), `fn` is executed as-is
 * with the existing deadline — inner layers MUST NOT extend the budget
 * their caller has set.
 */
export function withRetryDeadline<T>(
    budgetMs: number,
    fn: () => Promise<T>,
): Promise<T> {
    if (store.getStore()) {
        return fn()
    }
    return store.run({ deadline: Date.now() + budgetMs }, fn)
}

/**
 * True when the current deadline (if any) has already passed.
 */
export function isRetryDeadlineExpired(): boolean {
    const deadline = getRetryDeadline()
    return deadline !== undefined && Date.now() > deadline
}
