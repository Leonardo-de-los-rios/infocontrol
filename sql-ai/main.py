import os

import psycopg2
from dotenv import load_dotenv
from openai import OpenAI


def get_api_keys():
    api_keys = [
        value for key, value in os.environ.items() if key.startswith("API_KEY_")
    ]

    return api_keys


def get_tables(cursor):
    cursor.execute(
        """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public';
    """
    )
    return cursor.fetchall()


def get_structure_tables(cursor, tables):
    db_structure = ""

    for table in tables:
        table_name = table[0]
        db_structure += f"Table: {table_name}\n"
        cursor.execute(
            f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s;
        """,
            (table_name,),
        )
        columns = cursor.fetchall()
        for column in columns:
            db_structure += (
                f"  Column: {column[0]}, Type: {column[1]}, Nullable: {column[2]}\n"
            )
        db_structure += "\n"

    return db_structure


def process_response(response):
    if r"\_" in response:
        response = response.replace(r"\_", "_")
    return response


def query_openai(db_structure, user_prompt, api_keys):
    base_url = "https://api.aimlapi.com/v1"

    system_prompt = """
    Vas a ser un experto de SQL.
    Cualquier consulta que se te haga, tienes que convertirla a una consulta SQL.
    El formato de la consulta debe ser adecuado para ejecutarlo mediante la librería psycopg2 en Python.
    Ejemplo de formato: SELECT * FROM table_name WHERE column_name = 'value';
    Únicamente me tienes que devolver la consulta SQL, no me devuelvas otro texto.
    En el caso de que la consulta sea incorrecta en base a la información que te suministro,
    devuelveme un texto vacío, es decir, únicamente: ''.
    Te recomiento que primero traduzcas la consulta del usuario a Inglés y luego realices la consulta SQL correspondiente.
    Esta es la información de la estructura de las tablas de la base de datos:\n
    """
    system_prompt += db_structure

    print("User:", user_prompt)

    response = ""
    i = 0

    while i < len(api_keys):
        try:
            api_key = api_keys[i]
            api = OpenAI(api_key=api_key, base_url=base_url)

            completion = api.chat.completions.create(
                model="mistralai/Mistral-7B-Instruct-v0.2",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.7,
                max_tokens=256,
            )

            response = process_response(completion.choices[0].message.content)

            if response:
                print(f"AL_ML_API_{i+1}:", api_key)
                print("AI:", response)
                break
        except Exception as e:
            if e.body["statusCode"] == 429:
                print(f"La API_KEY {i} alcanzó su límite de consultas por hora.")
            else:
                print("Error:", e)

        i += 1

    return response


def query_db(cursor, query):
    try:
        cursor.execute(query)
        response = cursor.fetchall()
        print(response)
    except Exception as e:
        print("ERROR: Realizaste una consulta incorrecta")


def get_user_prompt():
    return "Decime los empleados que tienen más de 30 años"


if __name__ == "__main__":
    try:
        conn_string = os.getenv("API_KEY_NEON_DB")

        load_dotenv()

        # Establecer conexión
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()

        tables = get_tables(cursor)
        db_structure = get_structure_tables(cursor, tables)

        user_prompt = get_user_prompt()

        api_keys = get_api_keys()
        query = query_openai(db_structure, user_prompt, api_keys)
        query_db(cursor, query)

        # Cerrar cursor y conexión
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")
