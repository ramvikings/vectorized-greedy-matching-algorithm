import pandas as pd
import numpy as np


def forward_asof_match(
    tickets: pd.DataFrame,
    payments: pd.DataFrame,
    user_col: str,
    t_time_col: str,
    p_time_col: str,
    tolerance="24h"
) -> pd.DataFrame:
    t = tickets.copy()
    p = payments.copy()

    # Ensure datetime types
    t[t_time_col] = pd.to_datetime(t[t_time_col], errors="coerce", utc=True).dt.tz_localize(None)
    p[p_time_col] = pd.to_datetime(p[p_time_col], errors="coerce", utc=True).dt.tz_localize(None)

    # Sort for merge_asof
    t = t.sort_values([user_col, t_time_col], kind="mergesort")
    p = p.sort_values([user_col, p_time_col], kind="mergesort")

    # Forward join within tolerance
    out = pd.merge_asof(
        t,
        p,
        by=user_col,
        left_on=t_time_col,
        right_on=p_time_col,
        direction="forward",
        tolerance=pd.Timedelta(tolerance),
        suffixes=("", "_pay")
    )
    return out


def forward_asof_greedy_one_to_one(
    tickets: pd.DataFrame,
    payments: pd.DataFrame,
    user_col: str,
    t_time_col: str,
    p_time_col: str,
    tolerance="24h",
    keep_cols_t=None,
    keep_cols_p=None,
    suffix_t="_t",
    suffix_p="_p"
) -> pd.DataFrame:

    keep_cols_t = keep_cols_t or []
    keep_cols_p = keep_cols_p or []

    t = tickets.copy()
    p = payments.copy()

    # Normalize data
    t[user_col] = t[user_col].astype("string")
    p[user_col] = p[user_col].astype("string")

    t[t_time_col] = pd.to_datetime(t[t_time_col], errors="coerce", utc=True).dt.tz_localize(None)
    p[p_time_col] = pd.to_datetime(p[p_time_col], errors="coerce", utc=True).dt.tz_localize(None)

    t = t.dropna(subset=[user_col, t_time_col]).copy()
    p = p.dropna(subset=[user_col, p_time_col]).copy()

    # Stable sort
    t = t.sort_values([user_col, t_time_col], kind="mergesort").reset_index(drop=False).rename(columns={"index": "orig_idx_t"})
    p = p.sort_values([user_col, p_time_col], kind="mergesort").reset_index(drop=False).rename(columns={"index": "orig_idx_p"})

    t_uid = t[user_col].to_numpy()
    p_uid = p[user_col].to_numpy()
    t_time = t[t_time_col].to_numpy()
    p_time = p[p_time_col].to_numpy()

    tol = pd.Timedelta(tolerance).to_timedelta64()

    # Compute index ranges per user
    def ranges(arr):
        idx = {}
        n = len(arr)
        i = 0
        while i < n:
            j = i + 1
            while j < n and arr[j] == arr[i]:
                j += 1
            idx[arr[i]] = (i, j)
            i = j
        return idx

    t_ranges = ranges(t_uid)
    p_ranges = ranges(p_uid)

    users_both = np.intersect1d(t[user_col].unique(), p[user_col].unique())
    pairs = []

    for u in users_both:
        t_start, t_end = t_ranges[u]
        p_start, p_end = p_ranges[u]

        t_idx_slice = np.arange(t_start, t_end)
        p_idx_slice = np.arange(p_start, p_end)

        t_times = t_time[t_idx_slice]
        p_times = p_time[p_idx_slice]

        used = np.zeros(len(p_idx_slice), dtype=bool)
        j = 0
        for i_local, tt in enumerate(t_times):
            j = np.searchsorted(p_times, tt, side="left") if j == 0 else max(j, np.searchsorted(p_times, tt, side="left"))
            while j < len(p_times):
                if used[j]:
                    j += 1
                    continue
                if p_times[j] - tt > tol:
                    break
                used[j] = True
                pairs.append((t_idx_slice[i_local], p_idx_slice[j]))
                j += 1
                break

    if not pairs:
        return pd.DataFrame()

    ti, pi = map(np.array, zip(*pairs))
    t_match = t.iloc[ti].reset_index(drop=True)
    p_match = p.iloc[pi].reset_index(drop=True)

    cols_t = list(dict.fromkeys([user_col, t_time_col] + keep_cols_t))
    cols_p = list(dict.fromkeys([user_col, p_time_col] + keep_cols_p))

    out = pd.concat(
        [t_match[cols_t].add_suffix(suffix_t), p_match[cols_p].add_suffix(suffix_p)],
        axis=1
    )

    return out


if __name__ == "__main__":
    # --- Demo data ---
    tickets_demo = pd.DataFrame({
        "user_id": ["u1","u1","u2","u3","u3"],
        "interaction_time": [
            "2025-11-01 09:00", "2025-11-01 10:00",
            "2025-11-01 12:00",
            "2025-11-01 08:00", "2025-11-02 20:00"
        ],
        "channel": ["chat","email","chat","call","chat"]
    })

    payments_demo = pd.DataFrame({
        "user_id": ["u1","u1","u2","u3"],
        "payment_time": [
            "2025-11-01 09:30", "2025-11-02 09:00",
            "2025-11-01 14:00",
            "2025-11-01 09:15"
        ],
        "amount": [49, 99, 20, 15]
    })

    print("=== Quick merge_asof ===")
    print(forward_asof_match(
        tickets_demo, payments_demo,
        user_col="user_id",
        t_time_col="interaction_time",
        p_time_col="payment_time"
    ))

    print("\n=== Greedy one-to-one ===")
    print(forward_asof_greedy_one_to_one(
        tickets_demo, payments_demo,
        user_col="user_id",
        t_time_col="interaction_time",
        p_time_col="payment_time",
        keep_cols_t=["channel"],
        keep_cols_p=["amount"]
    ))
