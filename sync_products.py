import os
import requests
import psycopg2
from decimal import Decimal
from fastapi import FastAPI, HTTPException, Depends
from contextlib import contextmanager
from typing import Set

app = FastAPI()

# Configurações do ambiente
DB_URL = os.getenv('DB_URL')
API_PRODUCTS_URL = os.getenv('API_PRODUCTS_URL', "https://api.sigecloud.com.br/request/Produtos/GetAll")
API_TOKEN = os.getenv('API_TOKEN')
API_USER = os.getenv('API_USER')
API_APP = os.getenv('API_APP', "API")
PAGE_SIZE = os.getenv('PAGE_SIZE', "1000")

@contextmanager
def get_db_connection():
    """Gerenciador de conexão com o banco de dados"""
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()

def get_db_cursor():
    """Dependência para injeção de cursor"""
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

def get_existing_product_codes(cursor) -> Set[str]:
    """Retorna códigos de produtos já existentes no banco"""
    cursor.execute("SELECT codigo_produto FROM produtos")
    return {row[0] for row in cursor.fetchall()}

def fetch_products_from_api():
    """Busca produtos da API SigeCloud"""
    try:
        headers = {
            "Authorization-Token": API_TOKEN,
            "User": API_USER,
            "App": API_APP
        }
        params = {"pageSize": PAGE_SIZE}
        response = requests.get(API_PRODUCTS_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erro na API: {str(e)}")

def transform_product_data(api_data):
    """Transforma os dados da API para o formato do banco"""
    transformed = []
    
    if not api_data or 'data' not in api_data:
        return transformed
    
    for item in api_data['data']:
        try:
            transformed.append((
                str(item['ID']),
                item['Nome'],
                Decimal(str(item['PrecoVenda'])),
                item.get('Categoria', 'Sem Categoria')
            ))
        except (KeyError, ValueError) as e:
            continue  # Ignora produtos inválidos
            
    return transformed

def insert_new_products(cursor, new_products):
    """Insere novos produtos em lote"""
    if not new_products:
        return 0
    
    insert_query = """
        INSERT INTO produtos (
            codigo_produto,
            nome,
            preco,
            categoria
        ) VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(insert_query, new_products)
    return len(new_products)

@app.post("/sync-products")
async def sync_products(cursor = Depends(get_db_cursor)):
    """
    Endpoint para sincronização de produtos
    """
    try:
        # Passo 1: Obter produtos existentes
        existing_codes = get_existing_product_codes(cursor)
        
        # Passo 2: Buscar dados da API
        api_data = fetch_products_from_api()
        
        # Passo 3: Transformar dados
        all_products = transform_product_data(api_data)
        
        # Passo 4: Filtrar novos produtos
        new_products = [p for p in all_products if p[0] not in existing_codes]
        
        # Passo 5: Inserir novos registros
        inserted_count = insert_new_products(cursor, new_products)
        
        return {
            "status": "success",
            "inserted": inserted_count,
            "total_available": len(all_products)
        }
        
    except psycopg2.DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"Erro no banco: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")