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
API_URL = os.getenv('API_URL', "https://api.sigecloud.com.br/request/Pedidos/GetTodosPedidos")
API_TOKEN = os.getenv('API_TOKEN')
API_USER = os.getenv('API_USER')
API_APP = os.getenv('API_APP', "API")

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()

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

def fetch_clients_map(cursor) -> Dict[str, int]:
    """Obtém o mapeamento de emails de clientes para IDs"""
    cursor.execute("SELECT email, id FROM sugarart.public.clientes")
    return {row[0].lower(): row[1] for row in cursor.fetchall()}

def fetch_paginated_orders():
    """Busca todos os pedidos paginados da API"""
    all_orders = []
    page = 1
    headers = {
        "Authorization-Token": API_TOKEN,
        "User": API_USER,
        "App": API_APP
    }
    
    with requests.Session() as session:
        while True:
            try:
                response = session.get(
                    f"{API_URL}?page={page}",
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                if not data:
                    break
                    
                all_orders.extend(data)
                
                if len(data) < 100:  # Finaliza se última página
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Erro na requisição página {page}: {str(e)}"
                )
    
    return all_orders

def process_orders(orders: list, clients_map: Dict[str, int]):
    """Processa os pedidos para inserção no banco"""
    processed = []
    for order in orders:
        try:
            client_email = order.get('ClienteEmail', '').lower()
            if not client_email:
                continue
                
            client_id = clients_map.get(client_email)
            if not client_id:
                continue
                
            order_date = datetime.fromisoformat(
                order['Data'].replace("Z", "+00:00")
            ).date()
            
            processed.append((
                order['ID'],
                str(order.get('ValorFinal', 0)),
                order_date,
                order.get('Vendedor', ''),
                client_id
            ))
            
        except Exception as e:
            print(f"Erro processando pedido {order.get('ID')}: {str(e)}")
            continue
    
    return processed

@app.post("/sync-all-orders")
async def sync_all_orders(cursor = Depends(get_db_cursor)):
    """Endpoint para sincronização completa de pedidos"""
    try:
        # Passo 1: Obter mapeamento de clientes
        clients_map = fetch_clients_map(cursor)
        
        # Passo 2: Buscar todos os pedidos
        orders = fetch_paginated_orders()
        if not orders:
            return {"status": "success", "message": "Nenhum pedido encontrado na API"}
        
        # Passo 3: Processar pedidos
        processed = process_orders(orders, clients_map)
        
        # Passo 4: Inserir no banco
        if processed:
            query = """
            INSERT INTO sugarart.public.pedidos (
                codigo_pedido, valor_final, data_pedido, 
                vendedor, id_cliente
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (codigo_pedido) DO NOTHING
            """
            execute_batch(cursor, query, processed)
            
            return {
                "status": "success",
                "message": f"{len(processed)} pedidos sincronizados",
                "details": {
                    "total_encontrados": len(orders),
                    "processados": len(processed),
                    "ignorados": len(orders) - len(processed)
                }
            }
            
        return {"status": "success", "message": "Nenhum pedido válido para processar"}

    except psycopg2.DatabaseError as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro no banco de dados: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro inesperado: {str(e)}"
        )