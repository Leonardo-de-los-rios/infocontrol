import os

import google.generativeai as genai
import PIL.Image
from dotenv import load_dotenv

# Carga el archivo .env
load_dotenv()

# Ahora puedes acceder al API key como una variable de entorno
api_key = os.getenv("API_KEY")

genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-1.5-flash")

myfile = PIL.Image.open("captcha.jpg")

response = model.generate_content(
    [
        myfile,
        "\n\n",
        "Dime exactamente qué número aparece en la imagen, sin darme formato, únicamente quiero el número.",
    ]
)
print(response.text)
