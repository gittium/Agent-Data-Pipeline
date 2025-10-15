from google import generativeai
generativeai.configure(api_key="AIzaSyCyxeKeSKcTbXop9y2wD7jx5-1tIqaJMho")  
models = generativeai.list_models()  
for m in models:
    if "generateContent" in m.supported_generation_methods:
        print(m.name)
