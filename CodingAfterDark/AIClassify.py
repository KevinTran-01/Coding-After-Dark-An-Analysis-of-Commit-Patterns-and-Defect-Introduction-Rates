"""
Use AI to analyze commit messages instead of keyword matching
"""
import anthropic
import pandas as pd
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

INPUT_CSV = "updated_commits.csv"
OUTPUT_CSV = "ai_classified_commits.csv"

# Decide how many commits to send for each API call
# Bigger batch_size = cheaper + faster
# too much might be ineffective
BATCH_SIZE = 10

df = pd.read_csv(INPUT_CSV)

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM_PROMPT = """You are a software engineering researcher analyzing git commit messages.
For each commit message given, return a JSON classification.
Return ONLY a valid JSON array, no explanation, no markdown, no code blocks.
Each object in the array must have exactly these fields:
 
{
  "urgency": <integer 0-10>,
  "sentiment": <"negative" | "neutral" | "positive">,
  "category": <"bug_fix" | "feature" | "refactor" | "docs" | "test" | "chore" | "other">,
  "hedging": <true | false>,
  "clarity": <integer 0-10>,
  "rushed": <true | false>
}
 
Field definitions:
- urgency: 0=calm/routine, 10=extremely urgent/panicked
- sentiment: overall emotional tone of the message
- category: primary purpose of this commit
- hedging: true if author seems uncertain (words like "should work", "hopefully", "might fix")
- clarity: 0=cryptic/vague, 10=clear and descriptive
- rushed: true if commit seems hurried (very short message, typos, "wip", "temp", "quick fix")
"""

def buildprompt(messages):
    lines=[]
    for i, msg in enumerate(messages):
        truncated = str(messages)[:400].replace("\n"," ").strip() #truncate message to take only 400 words max
        lines.append(f'{i+1}. "{truncated}"')
    
    joined = "\n".join(lines)

    return f"Classify these {len(messages)} commit messages: \n\n {joined}"

def classify_batch(messages):
    prompt = buildprompt(messages)

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",  # cheapest + fastest
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )

    raw = response.content[0].text.strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1]          # remove opening fence
        if raw.startswith("json"):
            raw = raw[4:]                  # remove the word "json"
    raw = raw.strip()

    # Parse JSON response
    parsed = json.loads(raw)
 
    # Validate
    if len(parsed) != len(messages):
        raise ValueError(f"Expected {len(messages)} results, got {len(parsed)}")
 
    return parsed

results = []
total = len(df)
batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(batches):
    start = batch_num * BATCH_SIZE
    end = min(start+ BATCH_SIZE, total)
    batch = df.iloc[start:end]
    messages = batch["message"].tolist()
    shas = batch["sha"].tolist()

    try:
        classifications = classify_batch(messages)
        
        for sha, c in zip(shas, classifications):
            results.append({
                "sha":          sha,
                "ai_urgency":   c.get("urgency",   0),
                "ai_sentiment": c.get("sentiment", "neutral"),
                "ai_category":  c.get("category",  "other"),
                "ai_hedging":   c.get("hedging",   False),
                "ai_clarity":   c.get("clarity",   5),
                "ai_rushed":    c.get("rushed",    False),
            })
            
        pct = 100 * end / total
        print(f"  Batch {batch_num+1:4d}/{batches} ({pct:5.1f}%)  commits {start+1:,}-{end:,}  ")
        time.sleep(0.3)
 
    except json.JSONDecodeError as e:
        print(f" Batch {batch_num+1} — JSON parse error: {e} — skipping")
        time.sleep(1)
        continue
 
    except Exception as e:
        print(f" Batch {batch_num+1} — Error: {e} — retrying in 5s...")
        time.sleep(5)
 
        try:
            classifications = classify_batch(messages)
            for sha, c in zip(shas, classifications):
                results.append({
                    "sha":          sha,
                    "ai_urgency":   c.get("urgency",   0),
                    "ai_sentiment": c.get("sentiment", "neutral"),
                    "ai_category":  c.get("category",  "other"),
                    "ai_hedging":   c.get("hedging",   False),
                    "ai_clarity":   c.get("clarity",   5),
                    "ai_rushed":    c.get("rushed",    False),
                })

            print(f"  Batch {batch_num+1}")

        except Exception as e2:
            print(f"  Batch {batch_num+1} — retry failed: {e2} — skipping")
            continue
 
# MERGE AND SAVE
print(f"\nMerging {len(results):,} classifications into dataset...")
 
ai_df  = pd.DataFrame(results)
df_out = df.merge(ai_df, on="sha", how="left")
df_out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

# SUMMARY

classified = df_out["ai_urgency"].notna().sum()
print(f"\nDone. {classified:,} / {len(df_out):,} commits classified")
print(f"Saved to: {OUTPUT_CSV}")
 
print("\nClassification summary:")
print(f"  Avg urgency score:  {df_out['ai_urgency'].mean():.2f} / 10")
print(f"  Avg clarity score:  {df_out['ai_clarity'].mean():.2f} / 10")
print(f"  Rushed commits:     {df_out['ai_rushed'].sum():,} ({df_out['ai_rushed'].mean()*100:.1f}%)")
print(f"  Hedging commits:    {df_out['ai_hedging'].sum():,} ({df_out['ai_hedging'].mean()*100:.1f}%)")
 
print("\nCategory breakdown:")
for cat, count in df_out["ai_category"].value_counts().items():
    print(f"  {cat:15s} {count:6,}  ({100*count/len(df_out):.1f}%)")
 
print("\nSentiment breakdown:")
for sent, count in df_out["ai_sentiment"].value_counts().items():
    print(f"  {sent:15s} {count:6,}  ({100*count/len(df_out):.1f}%)")
 
print(f"\nNext step: update INPUT_CSV in 4_analyze.py to '{OUTPUT_CSV}' and re-run")