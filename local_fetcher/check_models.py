import os
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")

if not api_key:
    print("API Key not found in .env")
    exit()

print(f"Checking models for key: {api_key[:10]}...")

url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
try:
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        print("\n--- Available Models ---")
        found = False
        for m in data.get('models', []):
            if 'generateContent' in m.get('supportedGenerationMethods', []):
                print(f"- {m['name']}")
                found = True
        if not found:
            print("No models found supporting 'generateContent'.")
    else:
        print(f"\nError accessing API: {resp.status_code}")
        print(f"Response: {resp.text}")
except Exception as e:
    print(f"\nConnection failed: {e}")
