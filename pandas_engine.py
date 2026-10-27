"""Pandas fallback engine - used ONLY if Spark/Java cannot start on the host."""
import pandas as pd

NAME = "Pandas (fallback - Spark unavailable)"
REQUIRED = ["date", "ward", "waste_type", "kg_collected", "recyclable_kg",
            "scheduled_pickup", "actual_pickup"]


def load_and_clean(path):
    raw = pd.read_csv(path, dtype=str)
    raw.columns = [c.strip().lower() for c in raw.columns]
    missing = [c for c in REQUIRED if c not in raw.columns]
    if missing:
        raise ValueError("CSV is missing columns: " + ", ".join(missing))
    rows_in = len(raw)
    df = pd.DataFrame({
        "date": pd.to_datetime(raw["date"].str.strip(), errors="coerce").dt.date,
        "ward": raw["ward"].str.strip().str.title(),
        "waste_type": raw["waste_type"].str.strip().str.title(),
        "kg_collected": pd.to_numeric(raw["kg_collected"], errors="coerce"),
        "recyclable_kg": pd.to_numeric(raw["recyclable_kg"], errors="coerce").fillna(0.0),
        "scheduled_pickup": pd.to_datetime(raw["scheduled_pickup"], errors="coerce"),
        "actual_pickup": pd.to_datetime(raw["actual_pickup"], errors="coerce"),
    })
    df = df[df["date"].notna() & df["ward"].notna() & (df["ward"] != "")
            & df["waste_type"].notna() & (df["waste_type"] != "")
            & df["kg_collected"].notna() & (df["kg_collected"] > 0)].copy()
    df["recyclable_kg"] = df["recyclable_kg"].clip(lower=0).clip(upper=df["kg_collected"])
    df["delay_minutes"] = (df["actual_pickup"] - df["scheduled_pickup"]).dt.total_seconds() / 60.0
    return df.reset_index(drop=True), {"rows_in": rows_in, "rows_out": len(df), "rows_dropped": rows_in - len(df)}


def get_options(df):
    return sorted(df["ward"].unique()), sorted(df["waste_type"].unique()), df["date"].min(), df["date"].max()


def analyze(df, start, end, wards, types, threshold):
    f = df[(df["date"] >= start) & (df["date"] <= end)
           & df["ward"].isin(wards) & df["waste_type"].isin(types)].copy()
    f["delayed"] = (f["delay_minutes"] > threshold).astype(int)
    total, rec = float(f["kg_collected"].sum()), float(f["recyclable_kg"].sum())
    pickups, delayed = len(f), int(f["delayed"].sum())
    kpi = {"total_kg": total, "recyclable_kg": rec,
           "recycling_pct": (rec / total * 100) if total else 0.0,
           "areas": int(f["ward"].nunique()), "pickups": pickups, "delayed": delayed,
           "delay_rate": (delayed / pickups * 100) if pickups else 0.0}

    def agg(by):
        g = f.groupby(by, as_index=False).agg(total_kg=("kg_collected", "sum"),
                                              recyclable_kg=("recyclable_kg", "sum"))
        g["recycling_pct"] = (g["recyclable_kg"] / g["total_kg"] * 100).round(1)
        return g.sort_values("total_kg", ascending=False).reset_index(drop=True)

    by_ward = agg("ward")
    by_ward["non_recyclable_kg"] = by_ward["total_kg"] - by_ward["recyclable_kg"]
    by_type = agg("waste_type")
    by_date = f.groupby("date", as_index=False).agg(total_kg=("kg_collected", "sum")).sort_values("date")
    wd = f.groupby("ward", as_index=False).agg(pickups=("delayed", "size"),
                                               delayed_pickups=("delayed", "sum"),
                                               avg_delay_min=("delay_minutes", "mean"))
    wd["avg_delay_min"] = wd["avg_delay_min"].round(1)
    wd["delay_rate_pct"] = (wd["delayed_pickups"] / wd["pickups"] * 100).round(1)
    wd = wd.sort_values("delayed_pickups", ascending=False).reset_index(drop=True)
    delays = (f[f["delayed"] == 1][["date", "ward", "waste_type", "scheduled_pickup", "actual_pickup", "delay_minutes"]]
              .sort_values("delay_minutes", ascending=False).head(200).reset_index(drop=True))
    rows = f.drop(columns=["delayed"]).sort_values(["date", "ward"]).reset_index(drop=True)
    return {"kpi": kpi, "by_ward": by_ward, "by_type": by_type, "by_date": by_date,
            "ward_delay": wd, "delays": delays, "rows": rows}
