import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Depends
from contextlib import contextmanager

app = FastAPI()

# Configurações do banco de dados e API
DB_URL = os.getenv('DB_URL')
API_URL = os.getenv('API_URL', "https://api.sigecloud.com.br/request/Pedidos/Pesquisar")
API_TOKEN = os.getenv('API_TOKEN')
API_USER = os.getenv('API_USER')
API_APP = os.getenv('API_APP', "API")

@contextmanager
def get_db_connection():
    """Gerenciador de conexão com o banco de dados"""
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()

def get_db_cursor():
    """Fornece cursor transacional com commit/rollback automático"""
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

def fetch_today_orders():
    """Busca pedidos da API a partir da data atual"""
    today = datetime.now().strftime("%Y-%m-%d")
    headers = {
        "Authorization-Token": API_TOKEN,
        "User": API_USER,
        "App": API_APP
    }
    try:
        response = requests.get(API_URL, headers=headers, params={"dataInicial": today})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erro na API: {str(e)}")

def upsert_cliente(cursor, email, nome):
    """Realiza UPSERT de clientes e retorna o ID"""
    upsert_sql = """
    INSERT INTO sugarart.public.clientes (email, nome)
    VALUES (%s, %s)
    ON CONFLICT (email) DO UPDATE SET
        nome = EXCLUDED.nome
    RETURNING id
    """
    try:
        cursor.execute(upsert_sql, (email.lower(), nome or ''))
        return cursor.fetchone()[0]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao upsert cliente: {str(e)}")

def process_order(order, cursor):
    """Processa um pedido individual e retorna dados formatados"""
    client_email = order.get("ClienteEmail")
    if not client_email:
        return None

    try:
        # UPSERT do cliente
        client_id = upsert_cliente(cursor, client_email, order.get("Cliente", ""))

        # Formatação da data
        order_date = datetime.fromisoformat(order['Data'].replace("Z", "+00:00"))

        return (
            order['ID'],
            order.get('ValorFinal', 0.0),
            order_date,
            order.get('Vendedor', ''),
            client_id
        )
    except KeyError as e:
        print(f"Pedido {order.get('ID')} inválido: Campo faltando {str(e)}")
        return None

def upsert_pedidos(cursor, orders):
    """Realiza UPSERT em lote dos pedidos"""
    upsert_sql = """
    INSERT INTO sugarart.public.pedidos (
        codigo_pedido, valor_final, data_pedido, vendedor, id_cliente
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (codigo_pedido) DO UPDATE SET
        valor_final = EXCLUDED.valor_final,
        data_pedido = EXCLUDED.data_pedido,
        vendedor = EXCLUDED.vendedor,
        id_cliente = EXCLUDED.id_cliente
    """
    try:
        execute_batch(cursor, upsert_sql, orders)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao upsert pedidos: {str(e)}")

@app.post("/sync-orders")
async def sync_orders(cursor = Depends(get_db_cursor)):
    """Endpoint principal para sincronização de pedidos"""
    try:
        # Buscar pedidos do dia
        orders = fetch_today_orders()
        if not orders:
            return {"status": "success", "message": "Nenhum pedido encontrado para hoje"}

        # Processar pedidos
        processed_orders = []
        for order in orders:
            processed = process_order(order, cursor)
            if processed:
                processed_orders.append(processed)

        # Atualizar pedidos se houver dados válidos
        if processed_orders:
            upsert_pedidos(cursor, processed_orders)
            msg = f"{len(processed_orders)} pedidos sincronizados"
        else:
            msg = "Nenhum pedido válido para processar"

        return {"status": "success", "message": msg}

    except psycopg2.DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"Erro no banco de dados: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")