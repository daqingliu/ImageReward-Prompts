import pandas as pd

def process_parquet_file(data_dir, out_dir):
    # Read the original Parquet file
    df = pd.read_parquet(data_dir)

    # Keep only the relevant columns
    df = df[['prompt_id', 'prompt', 'classification']]

    # Remove duplicate rows
    df = df.drop_duplicates()
    
    # Rebuild index
    df.reset_index(drop=True, inplace=True)

    # Rename the 'prompt' field to 'Prompt'
    df.rename(columns={'prompt': 'Prompt'}, inplace=True)

    # Save to a new Parquet file
    df.to_parquet(out_dir, index=False)

# List of splits
splits = ['train', 'validation', 'test']

# Loop through each split and process the corresponding Parquet files
for split in splits:
    data_dir = f"/datasets/ImageRewardDB/metadata-{split}.parquet"
    out_dir = f"/datasets/ImageReward-Prompts/{split}.parquet"
    process_parquet_file(data_dir, out_dir)
