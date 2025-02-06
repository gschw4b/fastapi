import os
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from contextlib import contextmanager
from typing import Dict

app = FastAPI()

# Configurações
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

def fetch_todays_orders():
    """Busca pedidos da API a partir da data atual"""
    today = datetime.now().strftime("%Y-%m-%d")
    headers = {
        "Authorization-Token": API_TOKEN,
        "User": API_USER,
        "App": API_APP
    }
    try:
        response = requests.get(
            API_URL,
            headers=headers,
            params={"dataInicial": today},
            timeout=15
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erro na API: {str(e)}")

def get_clients_map(cursor) -> Dict[str, int]:
    """Mapeamento de emails para IDs de clientes"""
    cursor.execute("SELECT email, id FROM sugarart.public.clientes")
    return {row[0].lower(): row[1] for row in cursor.fetchall()}

def process_order(order: dict, clients_map: Dict[str, int]):
    """Processa um pedido individual"""
    try:
        # Obter email do cliente
        client_email = order.get('ClienteEmail', '').lower()
        if not client_email:
            return None
            
        # Buscar ID do cliente
        client_id = clients_map.get(client_email)
        if not client_id:
            return None
            
        # Converter data
        data_pedido = datetime.fromisoformat(order['Data'].replace("Z", "+00:00")).date()
        
        return (
            order['ID'],                       # codigo_pedido
            str(order.get('ValorFinal', 0)),   # valor_final
            data_pedido,                       # data_pedido
            order.get('Vendedor', ''),         # vendedor
            client_id                          # id_cliente
        )
    except Exception as e:
        print(f"Erro processando pedido {order.get('ID')}: {str(e)}")
        return None

def upsert_orders(cursor, orders: list):
    """Atualiza/insere pedidos em lote"""
    query = """
    INSERT INTO sugarart.public.pedidos (
        codigo_pedido, valor_final, data_pedido, vendedor, id_cliente
    ) VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (codigo_pedido) DO UPDATE SET
        valor_final = EXCLUDED.valor_final,
        data_pedido = EXCLUDED.data_pedido,
        vendedor = EXCLUDED.vendedor,
        id_cliente = EXCLUDED.id_cliente
    """
    execute_batch(cursor, query, orders)

@app.post("/sync-orders")
async def sync_orders(cursor = Depends(get_db_cursor)):
    """Endpoint principal para sincronização diária"""
    try:
        # 1. Obter mapeamento de clientes
        clients_map = get_clients_map(cursor)
        
        # 2. Buscar pedidos do dia
        raw_orders = fetch_todays_orders()
        if not raw_orders:
            return {"status": "success", "message": "Nenhum pedido encontrado para hoje"}
        
        # 3. Processar pedidos
        processed = [order for order in (
            process_order(o, clients_map) for o in raw_orders
        ) if order is not None]
        
        # 4. Salvar no banco
        if processed:
            upsert_orders(cursor, processed)
            return {
                "status": "success",
                "message": f"{len(processed)} pedidos sincronizados",
                "details": {
                    "total_recebidos": len(raw_orders),
                    "processados": len(processed),
                    "ignorados": len(raw_orders) - len(processed)
                }
            }
            
        return {"status": "success", "message": "Nenhum pedido válido para processar"}

    except psycopg2.DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"Erro no banco: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")