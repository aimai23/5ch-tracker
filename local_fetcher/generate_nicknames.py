import os
import json
import glob
import re
import html
import logging
import requests
import datetime
from dotenv import load_dotenv

# Setup
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "dat_cache")
CONFIG_DIR = os.path.join(os.path.dirname(BASE_DIR), "config")
OUTPUT_FILE = os.path.join(CONFIG_DIR, "nickname_dictionary.json")

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load Env
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    logging.error("GEMINI_API_KEY is not set in .env")
    exit(1)

def parse_dat_file(filepath):
    """
    Reads a 5ch .dat file and extracts comment text.
    """
    comments = []
    try:
        # Try UTF-8 first (as written by main.py)
        with open(filepath, "r", encoding="utf-8") as f:
            text_data = f.read()
    except UnicodeDecodeError:
        try:
            # Fallback to CP932 just in case
            with open(filepath, "r", encoding="cp932") as f:
                text_data = f.read()
        except Exception as e:
            logging.warning(f"Failed to read {os.path.basename(filepath)}: {e}")
            return ""

    # [User Request] Exclude 1st post (Header/Title post)
    # 5ch dat format: Line 1 (index 0) is the first post. We skip it by using [1:].
    for line in text_data.splitlines()[1:]: 
        parts = line.split("<>")
        if len(parts) >= 4:
            msg = parts[3]
            
            # [User Request] Exclude specific noisy characters (e.g. 👹)
            if '👹' in msg:
                continue

            # Simple cleaning
            msg = re.sub(r"https?://[\w/:%#\$&\?\(\)~\.=\+\-]+", "", msg)
            msg = re.sub(r"<[^>]+>", " ", msg)
            clean_msg = html.unescape(msg)
            
            # Additional safety: Skip empty or very short messages
            if len(clean_msg.strip()) > 1:
                comments.append(clean_msg.strip())
    
    return "\n".join(comments)

def analyze_with_gemini(combined_text):
    """
    Sends combined text to Gemini to identify Ticker <-> Nickname pairs.
    """
    prompt = """
    You are a linguistic expert specialized in Japanese internet slang (2ch/5ch).
    Analyze the provided thread context and identify **Nicknames/Slang** used for US Stock Tickers or Companies.

    Target: US Stocks (e.g., NVDA, TSLA, AAPL, SOXL).
    Match them with the slang found in the text.

    Examples:
    - NVDA: "革ジャン", "謎半導体"
    - TSLA: "テスラ", "イーロン"
    - SOXL: "即する", "靴磨き"
    - AAPL: "林檎"

    OUTPUT FORMAT: JSON ONLY.
    {
      "NVDA": ["革ジャン", "謎半導体"],
      "TSLA": ["イーロン"]
    }

    Rules:
    - Only include clear mappings found or strongly implied in the text.
    - If no nicknames are found, return empty JSON {}.
    - Key must be the Ticker Symbol (UpperCase).
    - Value must be a list of Japanese strings.
    - Aggressively extract as many valid variations as possible from the provided text.
    """

    model_name = "gemini-2.5-flash-lite" # Flash has 1M+ context window
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    
    logging.info(f"Sending prompt with {len(combined_text)} characters...")

    payload = {
        "contents": [{"parts": [{"text": prompt + f"\n\nTEXT DATA:\n{combined_text}"}]}],
        "generationConfig": {"response_mime_type": "application/json"}
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=180)
        if resp.status_code == 200:
            result = resp.json()
            content = result["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(content)
        else:
            logging.error(f"Gemini API Error: {resp.status_code} - {resp.text}")
            return {}
    except Exception as e:
        logging.error(f"Request Failed: {e}")
        return {}

        return {}

def merge_dicts(main_dict, new_dict):
    for ticker, nicks in new_dict.items():
        ticker = ticker.upper()
        if ticker not in main_dict:
            main_dict[ticker] = set()
        for n in nicks:
            main_dict[ticker].add(n)

def main():
    logging.info("Starting Nickname Generation (Batch Mode)...")
    
    # 1. Get all .dat files
    dat_files = glob.glob(os.path.join(CACHE_DIR, "*.dat"))
    if not dat_files:
        logging.error("No .dat files found in dat_cache.")
        return

    logging.info(f"Found {len(dat_files)} files. Reading and combining...")
    
    all_text_chunks = []
    total_chars = 0
    # Reduced further to improve reliability and prevent timeouts
    MAX_CHARS = 120000 

    # 2. Process each file locally
    for i, filepath in enumerate(dat_files):
        filename = os.path.basename(filepath)
        text = parse_dat_file(filepath)
        
        if not text or len(text) < 100:
            continue
            
        # Limit per file 
        trimmed_text = text[:30000] 
        all_text_chunks.append(f"--- Thread: {filename} ---\n{trimmed_text}")
        total_chars += len(trimmed_text)

    if not all_text_chunks:
        logging.info("No content found.")
        return

    # 3. Batch Send
    master_dict = {}
    current_batch = []
    current_size = 0
    
    batches = []
    
    for chunk in all_text_chunks:
        if current_size + len(chunk) > MAX_CHARS:
            batches.append("\n".join(current_batch))
            current_batch = [chunk]
            current_size = len(chunk)
        else:
            current_batch.append(chunk)
            current_size += len(chunk)
            
    if current_batch:
        batches.append("\n".join(current_batch))
        
    logging.info(f"Processing in {len(batches)} batch(es) to respect Rate Limits.")
    
    for i, batch_text in enumerate(batches):
        logging.info(f"Analyzing Batch {i+1}/{len(batches)} ({len(batch_text)} chars)...")
        result = analyze_with_gemini(batch_text)
        if result:
            logging.info(f"  -> Batch {i+1} found: {len(result)} tickers")
            merge_dicts(master_dict, result)
        else:
            logging.info(f"  -> Batch {i+1} yielded no results.")
            
        # Wait to avoid TPM (Tokens Per Minute) limit
        if i < len(batches) - 1:
            logging.info("Waiting 20s for rate limit cool-down...")
            import time
            time.sleep(20)

    # 4. Save Result
    final_output = {k: list(v) for k, v in master_dict.items()}
    sort_output = dict(sorted(final_output.items()))
    
    os.makedirs(CONFIG_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(sort_output, f, ensure_ascii=False, indent=2)
        
    logging.info(f"Saved nickname dictionary to: {OUTPUT_FILE}")
    logging.info("Done.")

if __name__ == "__main__":
    main()
