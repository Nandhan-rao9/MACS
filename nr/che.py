import os
from google import genai
from dotenv import load_dotenv

load_dotenv()

# Initialize the client with the new SDK
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
print(os.getenv("GOOGLE_API_KEY"))
print("Checking available models for your API key...")
print("-" * 50)

try:
    # List models using the new SDK syntax
    for model in client.models.list():
        # Check if the model can actually 'talk' (generateContent)
        if "generateContent" in model.supported_actions:
            print(f"Model ID: {model.name}")
            print(f"Capabilities: {model.supported_actions}")
            print("-" * 50)
except Exception as e:
    print(f"Failed to list models. Error: {e}")