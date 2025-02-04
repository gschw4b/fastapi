import os
import requests
import psycopg2
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from contextlib import contextmanager
import re

app = FastAPI()

# Configurações do banco de dados e API
DB_URL = os.getenv('DB_URL')
API_URL = os.getenv('API_URL', "https://api.sigecloud.com.br/request/Pessoas/Pesquisar")
API_TOKEN = os.getenv('API_TOKEN')
API_USER = os.getenv('API_USER')
API_APP = os.getenv('API_APP', "API")

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
def clean_phone(phone):
    """
    Remove parênteses, espaços e caracteres não numéricos de um número de telefone.
    """
    if not phone:
        return None
    return re.sub(r"[()\s-]", "", phone)

def fetch_updated_customers():
    today = datetime.now().strftime("%Y-%m-%d")
    headers = {
        "Authorization-Token": API_TOKEN,
        "User": API_USER,
        "App": API_APP
    }
    response = requests.get(f"{API_URL}?alteradoapos={today}", headers=headers)
    response.raise_for_status()
    return response.json()

def upsert_customers(cursor, customers):
    upsert_query = """
    INSERT INTO sugarart.public.clientes (
        razao_social, fantasia, email, inscricao_federal, telefone, celular,
        pais, uf, cep, bairro, logradouro, numero, complemento, cidade
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (email) DO UPDATE SET
        razao_social = EXCLUDED.razao_social,
        fantasia = EXCLUDED.fantasia,
        inscricao_federal = EXCLUDED.inscricao_federal,
        telefone = EXCLUDED.telefone,
        celular = EXCLUDED.celular,
        pais = EXCLUDED.pais,
        uf = EXCLUDED.uf,
        cep = EXCLUDED.cep,
        bairro = EXCLUDED.bairro,
        logradouro = EXCLUDED.logradouro,
        numero = EXCLUDED.numero,
        complemento = EXCLUDED.complemento,
        cidade = EXCLUDED.cidade
    """
    cursor.executemany(upsert_query, customers)

# Endpoint principal
@app.post("/sync-customers")
async def sync_customers(cursor = Depends(get_db_cursor)):
    try:
        customers_data = fetch_updated_customers()
        
        if not customers_data:
            return {"status": "success", "message": "Nenhum cliente atualizado encontrado"}

        customers_to_upsert = []
        for customer in customers_data:
            email = customer.get('Email')
            if not email:
                continue

            # Limpa os campos de telefone e celular
            telefone = clean_phone(customer.get('Telefone'))
            celular = clean_phone(customer.get('Celular'))

            customer_data = (
                customer.get('RazaoSocial'),         # razao_social
                customer.get('NomeFantasia'),        # fantasia
                email.lower(),                       # email
                customer.get('CNPJ_CPF'),            # inscricao_federal
                telefone,                            # telefone (limpo)
                celular,                             # celular (limpo)
                customer.get('Pais'),                # pais
                customer.get('UF'),                  # uf
                customer.get('CEP'),                 # cep
                customer.get('Bairro'),              # bairro
                customer.get('Logradouro'),          # logradouro
                customer.get('LogradouroNumero'),    # numero
                customer.get('Complemento'),         # complemento
                customer.get('Cidade')               # cidade
            )
            customers_to_upsert.append(customer_data)

        if customers_to_upsert:
            upsert_customers(cursor, customers_to_upsert)
            return {"status": "success", "message": f"{len(customers_to_upsert)} clientes atualizados/inseridos"}
        return {"status": "success", "message": "Nenhum cliente para atualizar/inserir"}

    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Erro na API: {str(e)}")
    except psycopg2.DatabaseError as e:
        raise HTTPException(status_code=500, detail=f"Erro no banco: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")