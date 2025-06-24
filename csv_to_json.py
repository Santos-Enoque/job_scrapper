import pandas as pd
import json

def csv_to_json(csv_file, json_file):
    """Convert CSV file to JSON with proper formatting."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file, encoding='utf-8-sig')
        
        # Convert to JSON format
        json_data = df.to_dict('records')
        
        # Write to JSON file with proper formatting
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Successfully converted {csv_file} to {json_file}")
        print(f"📊 Total jobs: {len(json_data)}")
        
        # Show a sample of the first job for verification
        if json_data:
            print("\n📋 Sample job (first entry):")
            print(json.dumps(json_data[0], ensure_ascii=False, indent=2))
            
    except Exception as e:
        print(f"❌ Error converting file: {e}")

if __name__ == "__main__":
    # Convert the emprego CSV to JSON
    csv_to_json("emprego_mz_jobs.csv", "emprego_mz_jobs.json") 