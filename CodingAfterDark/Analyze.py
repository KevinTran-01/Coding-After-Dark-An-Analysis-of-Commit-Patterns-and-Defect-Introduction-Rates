"""
STEP 4 — Analysis
Reads commits_ai_classified.csv and performs:
  4a - Basic statistics and sanity checks
  4b - Statistical hypothesis testing
  4c - Visualizations

  Made by AI, not me
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import os

matplotlib.use("Agg")

# CONFIG
INPUT_CSV = "ai_classified_commits.csv"
PLOTS_DIR = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)

BUCKET_ORDER = [
    "0-4: late night",
    "4-9: early morning",
    "9-17: work hours",
    "evening",
    "night"
]

BUCKET_COLORS = {
    "0-4: late night":    "#e74c3c",
    "4-9: early morning": "#e67e22",
    "9-17: work hours":   "#2ecc71",
    "evening":            "#3498db",
    "night":              "#9b59b6"
}


# LOAD DATA
df = pd.read_csv(INPUT_CSV)
df = df.dropna(subset=["time_bucket", "ai_urgency"])
df = df[df["time_bucket"].isin(BUCKET_ORDER)]

print(f"\nLoaded {len(df):,} commits\n")


# 4A — BASIC STATISTICS
print("4A — BASIC STATISTICS\n")

# Timezone resolution
total    = len(df)
resolved = df["timezone_resolved"].sum()
print(f"\nTimezone resolved:  {resolved:,} / {total:,} ({100*resolved/total:.1f}%)")
print(f"UTC fallback:       {total - resolved:,} commits")

# Time bucket distribution
print("\nCommits per time bucket:")
for bucket in BUCKET_ORDER:
    count = len(df[df["time_bucket"] == bucket])
    pct   = 100 * count / total
    print(f"  {bucket:25s} {count:6,}  ({pct:.1f}%)")

# Repos covered
print(f"\nRepos covered:      {df['repo'].nunique()}")
print(f"Unique authors:     {df['author_login'].nunique()}")
print(f"Date range:         {df['utc_timestamp'].min()[:10]} to {df['utc_timestamp'].max()[:10]}")

# AI classification summary
print("\nAI Classification Summary:")
print(f"  Avg urgency score:  {df['ai_urgency'].mean():.2f} / 10")
print(f"  Avg clarity score:  {df['ai_clarity'].mean():.2f} / 10")
print(f"  Rushed commits:     {df['ai_rushed'].sum():,} ({df['ai_rushed'].mean()*100:.1f}%)")
print(f"  Hedging commits:    {df['ai_hedging'].sum():,} ({df['ai_hedging'].mean()*100:.1f}%)")

print("\nCategory breakdown:")
for cat, count in df["ai_category"].value_counts().items():
    print(f"  {cat:15s} {count:6,}  ({100*count/total:.1f}%)")


# 4B — STATISTICAL HYPOTHESIS TESTING
print("\n4B — STATISTICAL HYPOTHESIS TESTING\n")

late_night = df[df["time_bucket"] == "0-4: late night"]
core_hours = df[df["time_bucket"] == "9-17: work hours"]

print(f"\nComparing:")
print(f"  Late night  (00:00-04:00): {len(late_night):,} commits")
print(f"  Core hours  (09:00-17:00): {len(core_hours):,} commits")

def mannwhitney(group_a, group_b, feature, label):
    a = group_a[feature].dropna()
    b = group_b[feature].dropna()
    stat, p = stats.mannwhitneyu(a, b, alternative="two-sided")
    effect  = 1 - (2 * stat) / (len(a) * len(b))
    sig     = "SIGNIFICANT" if p < 0.05 else "not significant"
    print(f"\n  {label}")
    print(f"    Late night mean:  {a.mean():.3f}")
    print(f"    Core hours mean:  {b.mean():.3f}")
    print(f"    p-value:          {p:.4f}  {sig}")
    print(f"    Effect size (r):  {effect:.3f}")
    return p

def chisquare(group_a, group_b, feature, label):
    a_yes = int(group_a[feature].sum())
    a_no  = len(group_a) - a_yes
    b_yes = int(group_b[feature].sum())
    b_no  = len(group_b) - b_yes
    chi2, p, _, _ = stats.chi2_contingency([[a_yes, a_no], [b_yes, b_no]])
    sig   = "SIGNIFICANT" if p < 0.05 else "not significant"
    print(f"\n  {label}")
    print(f"    Late night rate:  {a_yes/len(group_a)*100:.1f}%")
    print(f"    Core hours rate:  {b_yes/len(group_b)*100:.1f}%")
    print(f"    p-value:          {p:.4f}  {sig}")
    return p

print("\n--- Continuous Features (Mann-Whitney U) ---")
p_urgency  = mannwhitney(late_night, core_hours, "ai_urgency",  "Urgency Score")
p_clarity  = mannwhitney(late_night, core_hours, "ai_clarity",  "Clarity Score")
p_msglen   = mannwhitney(late_night, core_hours, "message_length", "Message Length")

print("\n--- Binary Features (Chi-Square) ---")
p_rushed   = chisquare(late_night, core_hours, "ai_rushed",  "Rushed Commits")
p_hedging  = chisquare(late_night, core_hours, "ai_hedging", "Hedging Language")

# Bonferroni correction
bonferroni = 0.05 / 6
print(f"\n--- Bonferroni Corrected Threshold: p < {bonferroni:.4f} ---")
all_results = {
    "Urgency Score":    p_urgency,
    "Clarity Score":    p_clarity,
    "Message Length":   p_msglen,
    "Rushed Commits":   p_rushed,
    "Hedging Language": p_hedging,
}
for test, p in all_results.items():
    status = "survives correction" if p < bonferroni else "❌ does not survive"
    print(f"  {test:20s} p={p:.4f}  {status}")

print("\n4C — GENERATING VISUALIZATIONS\n")

sns.set_theme(style="whitegrid")

# Plot 1: Commit activity by hour
print("\n  Plot 1: Commit activity by hour...")
hour_col = "local_hour" if df["timezone_resolved"].sum() > len(df) * 0.3 else "utc_hour"
hourly   = df[hour_col].value_counts().sort_index()

fig, ax = plt.subplots(figsize=(14, 5))
colors = ["#e74c3c" if h < 4 else "#3498db" if 9 <= h < 17 else "#95a5a6"
          for h in hourly.index]
ax.bar(hourly.index, hourly.values, color=colors, edgecolor="white", linewidth=0.5)
ax.axvspan(-0.5, 3.5,  alpha=0.08, color="red",  label="Late night (00-04)")
ax.axvspan(8.5,  16.5, alpha=0.08, color="blue", label="Core hours (09-17)")
ax.set_xlabel("Hour of Day", fontsize=12)
ax.set_ylabel("Number of Commits", fontsize=12)
ax.set_title("Commit Activity by Hour of Day", fontsize=14, fontweight="bold")
ax.set_xticks(range(0, 24))
ax.legend()
plt.tight_layout()
plt.savefig(f"{PLOTS_DIR}/1_commit_activity_by_hour.png", dpi=150)
plt.close()
print(f"  Saved: 1_commit_activity_by_hour.png")

# Plot 2: Urgency score by time bucket
print("  Plot 2: Urgency by time bucket...")
fig, ax = plt.subplots(figsize=(10, 6))
plot_df = df[df["time_bucket"].isin(BUCKET_ORDER)]
sns.boxplot(
    data=plot_df, x="time_bucket", y="ai_urgency",
    order=BUCKET_ORDER,
    palette=[BUCKET_COLORS[b] for b in BUCKET_ORDER],
    showfliers=False, ax=ax
)
ax.set_xlabel("Time Bucket", fontsize=12)
ax.set_ylabel("AI Urgency Score (0-10)", fontsize=12)
ax.set_title("Commit Urgency Score by Time of Day", fontsize=14, fontweight="bold")
ax.set_xticklabels(ax.get_xticklabels(), rotation=15, ha="right")
plt.tight_layout()
plt.savefig(f"{PLOTS_DIR}/2_urgency_by_bucket.png", dpi=150)
plt.close()
print(f"  Saved: 2_urgency_by_bucket.png")

# Plot 3: Clarity score by time bucket
print("  Plot 3: Clarity by time bucket...")
fig, ax = plt.subplots(figsize=(10, 6))
sns.boxplot(
    data=plot_df, x="time_bucket", y="ai_clarity",
    order=BUCKET_ORDER,
    palette=[BUCKET_COLORS[b] for b in BUCKET_ORDER],
    showfliers=False, ax=ax
)
ax.set_xlabel("Time Bucket", fontsize=12)
ax.set_ylabel("AI Clarity Score (0-10)", fontsize=12)
ax.set_title("Commit Message Clarity by Time of Day", fontsize=14, fontweight="bold")
ax.set_xticklabels(ax.get_xticklabels(), rotation=15, ha="right")
plt.tight_layout()
plt.savefig(f"{PLOTS_DIR}/3_clarity_by_bucket.png", dpi=150)
plt.close()
print(f"  Saved: 3_clarity_by_bucket.png")

# Plot 4: Rushed commit rate by time bucket
print("  Plot 4: Rushed rate by time bucket...")
rushed_rate = plot_df.groupby("time_bucket")["ai_rushed"].mean() * 100
rushed_rate = rushed_rate.reindex(BUCKET_ORDER).dropna()

fig, ax = plt.subplots(figsize=(10, 6))
colors = [BUCKET_COLORS[b] for b in rushed_rate.index]
bars = ax.bar(rushed_rate.index, rushed_rate.values, color=colors, edgecolor="white")
ax.bar_label(bars, fmt="%.1f%%", padding=3, fontsize=10)
ax.set_xlabel("Time Bucket", fontsize=12)
ax.set_ylabel("% of Commits Classified as Rushed", fontsize=12)
ax.set_title("Rushed Commit Rate by Time of Day", fontsize=14, fontweight="bold")
ax.set_xticklabels(rushed_rate.index, rotation=15, ha="right")
plt.tight_layout()
plt.savefig(f"{PLOTS_DIR}/4_rushed_by_bucket.png", dpi=150)
plt.close()
print(f"  Saved: 4_rushed_by_bucket.png")

# Plot 6: Urgency heatmap by hour and day of week
print("  Plot 6: Urgency heatmap by hour and day...")
df["day_name"] = pd.to_datetime(df["utc_timestamp"]).dt.day_name()
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
heatmap_data = df.groupby(["day_name", hour_col])["ai_urgency"].mean().unstack()
heatmap_data = heatmap_data.reindex(day_order)

fig, ax = plt.subplots(figsize=(16, 6))
sns.heatmap(
    heatmap_data, cmap="YlOrRd", ax=ax,
    cbar_kws={"label": "Avg Urgency Score"},
    linewidths=0.3
)
ax.set_xlabel("Hour of Day", fontsize=12)
ax.set_ylabel("Day of Week", fontsize=12)
ax.set_title("Average Commit Urgency — Hour × Day Heatmap", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig(f"{PLOTS_DIR}/6_urgency_heatmap.png", dpi=150)
plt.close()
print(f"  Saved: 6_urgency_heatmap.png")

# DONE
print("\nANALYSIS COMPLETE\n")
print("\nPlots explained:\n")
print("  1_commit_activity_by_hour.png  — when do developers commit?")
print("  2_urgency_by_bucket.png        — are late night commits more urgent?")
print("  3_clarity_by_bucket.png        — are late night messages less clear?")
print("  4_rushed_by_bucket.png         — are late night commits more rushed?")
print("  5_urgency_heatmap.png          — urgency by hour AND day of week")