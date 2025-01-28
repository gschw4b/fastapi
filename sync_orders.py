import os
import requests
import psycopg2
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from contextlib import contextmanager

app = FastAPI()

# Configurações do banco de dados e API
DB_URL = os.getenv('DB_URL')
API_URL = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"
API_TOKEN = os.getenv('API_TOKEN')
API_USER = os.getenv('API_USER')
API_APP = "API"

# Gerenciador de conexão com o banco
@contextmanager
def get_db_connection():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()

# Dependência para injeção de conexão
def get_db_cursor():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            cursor.close()

# Funções auxiliares
def get_existing_codes(cursor):
    cursor.execute("SELECT codigo_pedido FROM pedidos")
    return {row[0] for row in cursor.fetchall()}

def fetch_new_orders():
    today = datetime.now().strftime("%Y-%m-%d")
    headers = {
        "Authorization-Token": API_TOKEN,
        "User": API_USER,
        "App": API_APP
    }
    response = requests.get(API_URL, headers=headers, params={"dataInicial": today})
    response.raise_for_status()
    return response.json()

def insert_new_orders(cursor, new_orders):
    insert_query = """
    INSERT INTO pedidos (codigo_pedido, cliente, vendedor, data_envio, uf, periodicidade)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(insert_query, new_orders)

# Endpoint principal
@app.post("/sync-orders")
async def sync_orders(cursor = Depends(get_db_cursor)):
    print(f"Token teste: {API_TOKEN}")
    try:
        existing_codes = get_existing_codes(cursor)
        orders = fetch_new_orders()
        
        new_orders = []
        for order in orders:
            if order['ID'] not in existing_codes:
                new_orders.append((
                    order['ID'],
                    order.get('Cliente', ''),
                    order.get('Vendedor', ''),
                    order.get('DataEnvio'),
                    order.get('UF'),
                    order.get('Periodicidade')
                ))

        if new_orders:
            insert_new_orders(cursor, new_orders)
            return {"status": "success", "message": f"{len(new_orders)} novos pedidos inseridos"}
        return {"status": "success", "message": "Nenhum novo pedido encontrado"}

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erro na API: {str(e)}")
    except psycopg2.DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"Erro no banco: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")