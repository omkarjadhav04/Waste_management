"""Generates waste_data.csv (SAMPLE data, not real municipal records).
Run:  python generate_sample_data.py
"""
import csv
import random
from datetime import date, datetime, timedelta

random.seed(42)
WARDS = ["Ward A", "Ward B", "Ward C", "Ward D", "Ward E"]
TYPES = {  # waste_type: (min_kg, max_kg, min_recyclable_ratio, max_recyclable_ratio)
    "Plastic": (80, 200, 0.55, 0.85),
    "Paper": (60, 150, 0.70, 0.90),
    "Glass": (40, 120, 0.80, 0.95),
    "Metal": (30, 90, 0.85, 0.95),
    "Organic": (150, 350, 0.0, 0.0),
}

rows = []
start = date(2026, 9, 1)
for d in range(56):
    day = start + timedelta(days=d)
    for i, ward in enumerate(WARDS):
        for wtype in random.sample(list(TYPES), k=random.choice([1, 2])):
            lo, hi, rlo, rhi = TYPES[wtype]
            kg = random.randint(lo, hi)
            rec = round(kg * random.uniform(rlo, rhi))
            sched = datetime(day.year, day.month, day.day, 6 + i, 0)
            if random.random() < 0.12:
                delay = random.randint(20, 90)       # late pickup
            else:
                delay = random.randint(-5, 12)       # roughly on time
            actual = sched + timedelta(minutes=delay)
            rows.append([day.isoformat(), ward, wtype, kg, rec,
                         sched.strftime("%Y-%m-%d %H:%M"),
                         actual.strftime("%Y-%m-%d %H:%M")])

# A few deliberately dirty rows so the cleaning step has something to do
rows += [
    ["2026-10-05", "", "Plastic", 100, 60, "2026-10-05 07:00", "2026-10-05 07:05"],      # missing ward
    ["2026-10-06", "Ward A", "Paper", "abc", 50, "2026-10-06 06:00", "2026-10-06 06:10"],  # invalid kg
    ["not-a-date", "Ward B", "Glass", 70, 60, "2026-10-07 07:00", "2026-10-07 07:05"],     # invalid date
    ["2026-10-08", "Ward C", "Metal", -40, 30, "2026-10-08 08:00", "2026-10-08 08:10"],    # negative kg
    ["2026-10-09", "Ward D", "Plastic", 90, 150, "2026-10-09 09:00", "2026-10-09 09:20"],  # recyclable > total
]

with open("waste_data.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["date", "ward", "waste_type", "kg_collected", "recyclable_kg",
                "scheduled_pickup", "actual_pickup"])
    w.writerows(rows)
print(f"waste_data.csv written with {len(rows)} rows")
