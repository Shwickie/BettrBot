try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    print("OpenAI library not installed. Run: pip install openai")
    OPENAI_AVAILABLE = False
    OpenAI = None
client = OpenAI()  # reads OPENAI_API_KEY from the environment
r = client.responses.create(model="gpt-4o-mini", input="write a haiku about ai", store=True)
print(r.output_text)
