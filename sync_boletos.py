import os
import requests
import psycopg2
from decimal import Decimal
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends
from contextlib import contextmanager
from typing import List, Dict, Any
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configurações do ambiente
DB_URL = os.getenv('DB_URL')
API_BOLETOS_URL = os.getenv('API_BOLETOS_URL', "https://api.sigecloud.com.br/request/Boletos/Pesquisar")
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
        except Exception as e:
            conn.rollback()
            logger.error(f"Erro no banco de dados: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Erro no banco: {str(e)}")
        finally:
            cursor.close()

def fetch_clientes_mapping(cursor) -> Dict[str, int]:
    """Busca mapeamento de razão social para ID de clientes"""
    try:
        logger.info("Buscando mapeamento de clientes...")
        cursor.execute("SELECT id, razao_social FROM clientes")
        client_map = {razao.strip().lower(): cliente_id for cliente_id, razao in cursor.fetchall() if razao}
        logger.info(f"Mapeamento de {len(client_map)} clientes carregado")
        return client_map
    except Exception as e:
        logger.error(f"Erro ao buscar clientes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao buscar clientes: {str(e)}")

def fetch_boletos_from_api(
    page: int = 1,
    pageSize: int = int(PAGE_SIZE),
    data_inicial: str = datetime.now().isoformat()  # Data atual como valor padrão
) -> List[Dict[str, Any]]:
    """
    Busca dados de boletos da API com paginação e filtro por data inicial.
    """
    try:
        headers = {
            "Authorization-Token": API_TOKEN,
            "User": API_USER,
            "App": API_APP,
            "Content-Type": "application/json"
        }
        params = {
            'page': page,
            'pageSize': pageSize,
            'pago': 'false',  # Filtro para boletos não pagos
        }
        
        if data_inicial:
            params['dataInicial'] = data_inicial  # Adiciona o filtro de data inicial
        
        logger.info(f"Buscando boletos da API (página {page}) com data inicial: {data_inicial}...")
        response = requests.get(API_BOLETOS_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Boletos recebidos com sucesso (página {page})")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição à API: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro na API: {str(e)}")

def transform_boleto_data(api_data: List[Dict[str, Any]], clientes_map: Dict[str, int]) -> List[Dict[str, Any]]:
    """Transforma os dados da API para o formato do banco de dados"""
    transformed = []
    
    if not api_data:
        logger.warning("Resposta da API vazia ou inválida")
        return transformed
    
    for item in api_data:
        try:
            # Verifica se o item é válido
            if not item or 'Id' not in item:
                logger.warning(f"Ignorando item inválido: {item}")
                continue

            # Conversão de datas
            data_emissao = datetime.fromisoformat(item['DataEmissao']).date() if item.get('DataEmissao') else None
            data_vencimento = datetime.fromisoformat(item['DataVencimento']).date() if item.get('DataVencimento') else None
            
            # Tratamento do campo 'Sacado' para evitar None
            sacado = item.get('Sacado', '') or ''  # Garante que sacado seja uma string vazia se for None
            sacado = sacado.strip().lower()  # Agora podemos chamar .strip() com segurança
            id_cliente = clientes_map.get(sacado)

            transformed.append({
                'codigo_boleto': str(item['Id']),
                'numero_documento': item.get('NumeroDocumento', ''),
                'valor_boleto': Decimal(str(item.get('ValorBoleto', 0))),
                'id_cliente': id_cliente,
                'pago': bool(item.get('Pago', False)),
                'cancelado': bool(item.get('Cancelado', False)),
                'estornado': bool(item.get('Estornado', False)),
                'enviado': bool(item.get('RemessaEnviada', False)),
                'retorno_recebido': bool(item.get('RetornoRecebido', False)),
                'descricao': item.get('Descricao', '')[:255],  # Trunca se necessário
                'data_emissao': data_emissao,
                'data_vencimento': data_vencimento,
                'multa_apos_vencimento': Decimal(str(item.get('MultaAposVencimento', 0)))
            })
        except (KeyError, ValueError) as e:
            logger.warning(f"Erro ao processar boleto {item.get('Id')}: {str(e)}")
            continue  # Ignora boletos inválidos
            
    logger.info(f"{len(transformed)} boletos transformados com sucesso")
    return transformed

def upsert_boletos(cursor, boletos: List[Dict[str, Any]]):
    """Realiza UPSERT dos boletos no banco de dados"""
    if not boletos:
        logger.warning("Nenhum boleto para inserir/atualizar")
        return 0
    
    upsert_query = """
        INSERT INTO boletos (
            codigo_boleto, numero_documento, valor_boleto, id_cliente,
            pago, cancelado, estornado, enviado, retorno_recebido,
            descricao, data_emissao, data_vencimento, multa_apos_vencimento
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (codigo_boleto) DO UPDATE SET
            numero_documento = EXCLUDED.numero_documento,
            valor_boleto = EXCLUDED.valor_boleto,
            id_cliente = EXCLUDED.id_cliente,
            pago = EXCLUDED.pago,
            cancelado = EXCLUDED.cancelado,
            estornado = EXCLUDED.estornado,
            enviado = EXCLUDED.enviado,
            retorno_recebido = EXCLUDED.retorno_recebido,
            descricao = EXCLUDED.descricao,
            data_emissao = EXCLUDED.data_emissao,
            data_vencimento = EXCLUDED.data_vencimento,
            multa_apos_vencimento = EXCLUDED.multa_apos_vencimento
    """
    
    batch_data = [(
        boleto['codigo_boleto'],
        boleto['numero_documento'],
        boleto['valor_boleto'],
        boleto['id_cliente'],
        boleto['pago'],
        boleto['cancelado'],
        boleto['estornado'],
        boleto['enviado'],
        boleto['retorno_recebido'],
        boleto['descricao'],
        boleto['data_emissao'],
        boleto['data_vencimento'],
        boleto['multa_apos_vencimento']
    ) for boleto in boletos]

    try:
        logger.info("Iniciando inserção/atualização de boletos...")
        cursor.executemany(upsert_query, batch_data)
        logger.info(f"{len(batch_data)} boletos processados com sucesso")
        return len(batch_data)
    except Exception as e:
        logger.error(f"Erro ao inserir/atualizar boletos: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao inserir/atualizar boletos: {str(e)}")

@app.post("/sync-boletos")
async def sync_boletos(cursor = Depends(get_db_cursor)):
    """
    Endpoint para sincronização de boletos
    """
    try:
        # Passo 1: Obter mapeamento de clientes
        clientes_map = fetch_clientes_mapping(cursor)
        
        # Passo 2: Buscar dados da API
        api_data = fetch_boletos_from_api()
        
        # Passo 3: Transformar dados
        transformed_boletos = transform_boleto_data(api_data, clientes_map)
        
        # Passo 4: Realizar UPSERT dos boletos
        upserted_count = upsert_boletos(cursor, transformed_boletos)
        
        return {
            "status": "success",
            "upserted": upserted_count,
            "total_available": len(transformed_boletos)
        }
        
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")