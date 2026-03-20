from transformers import pipeline
import time

# Load both models
print("Loading old model (SST-2)...")
old_pipe = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=-1)
print("Loading new model (CoursifyQU)...")
new_pipe = pipeline("sentiment-analysis", model="CoursifyQU/student-review-sentiment", device=-1)

def old_sentiment(text):
    result = old_pipe(text[:2000])[0]
    raw_label = result["label"]
    confidence = result["score"]
    score = confidence if raw_label == "POSITIVE" else -confidence
    score = round(score, 4)
    if score > 0.85: label = "very positive"
    elif score > 0.5: label = "positive"
    elif score < -0.85: label = "very negative"
    elif score < -0.5: label = "negative"
    else: label = "neutral"
    return score, label

def new_sentiment(text):
    result = new_pipe(text[:2000])[0]
    raw_label = result["label"]
    confidence = result["score"]
    if raw_label == "positive": score = confidence
    elif raw_label == "negative": score = -confidence
    else: score = 0.0
    score = round(score, 4)
    if score > 0.85: label = "very positive"
    elif score > 0.3: label = "positive"
    elif score < -0.85: label = "very negative"
    elif score < -0.3: label = "negative"
    else: label = "neutral"
    return score, label

edge_cases = [
    # NEGATION
    ("would never take again", "negative"),
    ("not the best professor", "negative"),
    ("don't recommend this course", "negative"),
    ("wouldn't take this again", "negative"),
    ("does not explain concepts well", "negative"),
    # DOUBLE NEGATION (positive)
    ("can't complain about the grading", "positive"),
    ("never had a better professor", "positive"),
    ("nothing to complain about", "positive"),
    ("you won't regret taking this course", "positive"),
    # MIXED (neutral)
    ("lectures are boring but the exams are fair", "neutral"),
    ("hard course but fair grading", "neutral"),
    ("easy A but you won't learn anything", "neutral"),
    ("it's alright, nothing special", "neutral"),
    # STUDENT SLANG
    ("total bird course, easy A", "positive"),
    ("GPA booster for sure", "positive"),
    ("absolute weed-out course, brutal", "negative"),
    ("mid professor honestly", "negative"),
    ("dumpster fire of a class", "negative"),
    # SARCASM
    ("oh yeah great class if you love suffering", "negative"),
    ("I learned so much... about how not to teach", "negative"),
    ("fantastic way to destroy your GPA", "negative"),
    # STRONG
    ("worst class I've ever taken, avoid at all costs", "negative"),
    ("confusing lectures, unfair exams, terrible experience", "negative"),
    ("amazing professor, truly cares about students", "positive"),
    ("best course I've taken in my entire degree", "positive"),
    ("everyone should take this course", "positive"),
    # TRICKY
    ("hard course but you learn a lot", "positive"),
    ("easy but boring", "neutral"),
    ("passed but barely", "neutral"),
    # REAL-WORLD REVIEWS
    ("The lectures were okay and the material was fine but the final exam was unfair and grading was harsh", "negative"),
    ("Professor reads directly from slides, no engagement whatsoever", "negative"),
    ("Tough course but the professor makes it worth it, highly recommend", "positive"),
]

print(f"\n{'Text':<65} {'Expected':<12} {'Old Model':<25} {'New Model':<25} {'Old':>4} {'New':>4}")
print("=" * 145)

old_correct = 0
new_correct = 0

for text, expected in edge_cases:
    old_score, old_label = old_sentiment(text)
    new_score, new_label = new_sentiment(text)

    # Check if the broad category matches
    def matches(label, expected):
        if expected == "positive": return label in ("positive", "very positive")
        if expected == "negative": return label in ("negative", "very negative")
        if expected == "neutral": return label == "neutral"
        return False

    old_ok = "Y" if matches(old_label, expected) else "X"
    new_ok = "Y" if matches(new_label, expected) else "X"

    if matches(old_label, expected): old_correct += 1
    if matches(new_label, expected): new_correct += 1

    display_text = text[:63]
    print(f"{display_text:<65} {expected:<12} {old_label} ({old_score:+.3f}){'':<5} {new_label} ({new_score:+.3f}){'':<5} {old_ok:>4} {new_ok:>4}")

print("=" * 145)
print(f"{'ACCURACY':<65} {'':12} {'Old: ' + str(old_correct) + '/' + str(len(edge_cases)):<25} {'New: ' + str(new_correct) + '/' + str(len(edge_cases)):<25}")
