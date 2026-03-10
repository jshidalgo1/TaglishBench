import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import re

# Set visual style
sns.set_theme(style="whitegrid")

def load_data(db_path="taglishbench.db"):
    """Loads raw SQLite data into a Pandas DataFrame."""
    conn = sqlite3.connect(db_path)
    # Exclude empty text rows safely
    df = pd.read_sql_query("SELECT * FROM comments WHERE text IS NOT NULL AND text != ''", conn)
    conn.close()
    return df

def calculate_metrics(df):
    """Calculates heuristic quality metrics for each row."""
    print("Calculating quality metrics...")
    
    # 1. Word Count
    df['word_count'] = df['text'].apply(lambda x: len(str(x).split()))
    
    # 2. Alphanumeric Ratio (to catch emoji/symbol spam)
    def calc_alpha_ratio(text):
        text = str(text)
        if not text: return 0
        alphas = sum(c.isalnum() for c in text)
        return alphas / len(text)
        
    df['alpha_ratio'] = df['text'].apply(calc_alpha_ratio)
    
    # 3. URL Detection
    df['has_url'] = df['text'].apply(lambda x: 1 if re.search(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', str(x)) else 0)
    
    return df

def apply_gold_standard_filters(df):
    """Applies the agreed upon filters to separate gold-standard rows."""
    
    # Define our heuristics
    min_words = 10
    min_score = 1  # Must have at least 1 upvote/like
    min_alpha_ratio = 0.70  # At least 70% actual text characters
    
    # Determine which rows pass the test
    df['is_gold_standard'] = (
        (df['word_count'] >= min_words) &
        (df['engagement_score'] >= min_score) &
        (df['alpha_ratio'] >= min_alpha_ratio) &
        (df['has_url'] == 0)
    )
    
    return df

def generate_visualizations(df):
    """Generates and saves the analytical plots."""
    print("Generating visualizations...")
    
    # 1. Average Word Count per Source (Platform & Origin)
    plt.figure(figsize=(12, 6))
    # Group by origin but ensure we separate YouTube vs Reddit visually
    avg_words = df.groupby(['source', 'origin'])['word_count'].mean().reset_index()
    avg_words = avg_words.sort_values('word_count', ascending=False)
    
    sns.barplot(data=avg_words, x='word_count', y='origin', hue='source', dodge=False)
    plt.title('Average Comment Length (Word Count) by Source')
    plt.xlabel('Average Words per Comment')
    plt.ylabel('Origin (Channel/Subreddit)')
    plt.tight_layout()
    plt.savefig('avg_word_count.png')
    plt.close()

    # 2. Volume of "Gold Standard" Candidates
    plt.figure(figsize=(12, 8))
    
    # Count total vs gold
    counts = df.groupby(['origin']).agg(
        Total_Comments=('entry_id', 'count'),
        Gold_Standard=('is_gold_standard', 'sum')
    ).reset_index()
    
    # Melt for seaborn stacked representation
    counts_melted = counts.melt(id_vars='origin', var_name='Type', value_name='Count')
    counts_melted = counts_melted.sort_values(['origin', 'Type'], ascending=[True, False])

    sns.barplot(data=counts_melted, x='Count', y='origin', hue='Type')
    plt.title('Total Comments vs "Gold Standard" Viable Comments')
    plt.xlabel('Number of Comments')
    plt.ylabel('Origin (Channel/Subreddit)')
    plt.tight_layout()
    plt.savefig('gold_standard_volume.png')
    plt.close()

    # 3. Distribution of Engagement vs Length (for Gold Standard only)
    plt.figure(figsize=(10, 6))
    gold_df = df[df['is_gold_standard'] == True]
    
    # Use log scale since engagement can be massively skewed by viral posts
    sns.scatterplot(data=gold_df, x='word_count', y='engagement_score', alpha=0.5, hue='source')
    plt.yscale('symlog') 
    plt.title('Engagement Score vs Word Count (Gold Standard Subset)')
    plt.xlabel('Word Count')
    plt.ylabel('Engagement Score (Log Scale)')
    plt.tight_layout()
    plt.savefig('engagement_vs_length.png')
    plt.close()

def main():
    print("Loading SQLite dataset...")
    df = load_data()
    
    if len(df) == 0:
        print("Dataset is empty. Run scrapers first.")
        return
        
    print(f"Loaded {len(df)} total comments.")
    
    df = calculate_metrics(df)
    df = apply_gold_standard_filters(df)
    
    gold_count = df['is_gold_standard'].sum()
    print(f"Found {gold_count} comments natively matching the Gold Standard criteria ({(gold_count/len(df))*100:.1f}%).")
    
    generate_visualizations(df)
    print("Visualizations saved: avg_word_count.png, gold_standard_volume.png, engagement_vs_length.png")

if __name__ == "__main__":
    main()
