import imaplib
import email
import tempfile
import pandas as pd
import smtplib
import os
import uuid
import psycopg2
import requests
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.header import decode_header
from contextlib import contextmanager
from fastapi import FastAPI, HTTPException, Depends
from fastapi.concurrency import run_in_threadpool

app = FastAPI()

# Configurações gerais
DB_URL = os.getenv('DB_URL')
API_URL = "https://api.sigecloud.com.br/request/Pedidos/Pesquisar"
API_TOKEN = os.getenv('API_TOKEN')
API_USER = os.getenv('API_USER')
API_APP = "API"
IMAP_SERVER = os.getenv('IMAP_SERVER')
IMAP_PORT = int(os.getenv('IMAP_PORT', 993))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD')
SMTP_SERVER = os.getenv('SMTP_SERVER')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()

def get_db_cursor(conn = Depends(get_db_connection)):
    with conn.cursor() as cursor:
        try:
            yield cursor
            conn.commit()
        except:
            conn.rollback()
            raise

# Funções auxiliares para sincronização de pedidos
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
    params = {"dataInicial": today}

    try:
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"Erro HTTP: {e}\nResposta: {response.text}")
        raise
    except Exception as e:
        print(f"Erro inesperado: {e}")
        raise

def insert_new_orders(cursor, new_orders):
    cursor.executemany(
        "INSERT INTO pedidos (codigo_pedido, cliente, vendedor, data_envio, uf) VALUES (%s, %s, %s, %s, %s)",
        new_orders
    )

def conectar_imap():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    mail.login(EMAIL_USER, EMAIL_PASSWORD)
    return mail

def buscar_emails_nao_lidos(mail):
    mail.select('inbox')
    status, response = mail.search(None, 'UNSEEN')  # Buscar não lidos
    email_ids = response[0].split()
    return email_ids

def baixar_anexo(mail, email_id):
    status, response = mail.fetch(email_id, '(RFC822)')
    for response_part in response:
        if isinstance(response_part, tuple):
            msg = email.message_from_bytes(response_part[1])
            for part in msg.walk():
                content_disposition = str(part.get("Content-Disposition"))
                if 'attachment' in content_disposition:
                    filename = part.get_filename()
                    if filename.endswith('.xlsx'):
                        file_data = part.get_payload(decode=True)
                        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
                        temp_file.write(file_data)
                        temp_file.close()
                        return temp_file.name
    return None

def converter_xlsx_para_csv(caminho_arquivo_xlsx):
    try:
        df = pd.read_excel(caminho_arquivo_xlsx, sheet_name=None)  # Lê todas as abas
        csv_file = caminho_arquivo_xlsx.replace('.xlsx', '.csv')  # Nome do arquivo CSV
        df_combined = pd.concat(df.values(), ignore_index=True)
        df_combined.to_csv(csv_file, index=False, sep=';')
        return csv_file
    except Exception as e:
        print(f"Erro ao converter o arquivo .xlsx para .csv: {e}")
        return None

def enviar_email_com_anexo(to_email, subject, body, attachment_path):
    try:
        msg = MIMEMultipart()
        msg['From'] = EMAIL_USER
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        part = MIMEBase('application', 'octet-stream')
        with open(attachment_path, 'rb') as attachment:
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        
        filename = os.path.basename(attachment_path)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        msg.attach(part)
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_USER, to_email, msg.as_string())
            print(f"Email enviado para {to_email} com o anexo {filename}")
    except Exception as e:
        print(f"Erro ao enviar o e-mail: {e}")

def deletar_email(email_hash: str):
    try:
        # Conectar ao servidor IMAP
        mail = conectar_imap()
        
        # Selecionar a caixa de entrada
        mail.select('inbox')
        
        # Buscar emails com o hash no assunto
        status, response = mail.search(None, f'SUBJECT "{email_hash}"')
        if status != "OK":
            raise Exception("Email não encontrado.")
        
        email_ids = response[0].split()
        if not email_ids:
            raise Exception("Nenhum email com o hash especificado encontrado.")

        # Marcar como deletado na caixa de entrada
        for email_id in email_ids:
            mail.store(email_id, '+FLAGS', '\\Deleted')

        # Expurgar os emails deletados da caixa de entrada
        mail.expunge()
        
        # Agora movemos para a lixeira para garantir a exclusão definitiva
        mail.select('Trash')  # Ajuste a pasta 'Trash' se necessário
        for email_id in email_ids:
            mail.store(email_id, '+FLAGS', '\\Deleted')
        
        # Expurgar os emails deletados da lixeira
        mail.expunge()

        return f"Email com o hash {email_hash} deletado com sucesso."
    
    except Exception as e:
        return f"OK"
    
# Endpoints
@app.post("/sync-orders")
async def sync_orders(conn = Depends(get_db_connection)):
    return await run_in_threadpool(sync_orders_task, conn)

def sync_orders_task(conn):
    cursor = conn.cursor()
    try:
        existing_codes = get_existing_codes(cursor)
        orders = fetch_new_orders()
        
        new_orders = [
            (
                order['ID'],
                order.get('Cliente', ''),
                order.get('Vendedor', ''),
                order.get('DataEnvio'),
                order.get('UF')
             )
            for order in orders if order['ID'] not in existing_codes
        ]

        if new_orders:
            insert_new_orders(cursor, new_orders)
            return {"status": "success", "message": f"{len(new_orders)} novos pedidos inseridos"}
        return {"status": "success", "message": "Nenhum novo pedido encontrado"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/processar_email")
async def processar_email():
    try:
        # Conectar ao IMAP e buscar emails não lidos
        mail = conectar_imap()
        email_ids = buscar_emails_nao_lidos(mail)
        
        if email_ids:
            for email_id in email_ids:
                arquivo_xlsx = baixar_anexo(mail, email_id)
                if arquivo_xlsx:
                    print(f"Arquivo .xlsx baixado: {arquivo_xlsx}")
                    arquivo_csv = converter_xlsx_para_csv(arquivo_xlsx)
                    print(f"Arquivo convertido para .csv: {arquivo_csv}")

                    hash_aleatorio = uuid.uuid4().hex
                    
                    # Enviar de volta o arquivo convertido como anexo
                    enviar_email_com_anexo(
                        to_email=EMAIL_USER,  # Pode ajustar para o e-mail de destino
                        subject=f"Arquivo CSV - {hash_aleatorio}",
                        body="Aqui está o arquivo CSV convertido.",
                        attachment_path=arquivo_csv
                    )
                    return {"message": f"Email enviado com o anexo {arquivo_csv}"}
                else:
                    return HTTPException(status_code=404, detail="Nenhum anexo .xlsx encontrado no e-mail.")
        else:
            return HTTPException(status_code=404, detail="Nenhum e-mail não lido encontrado.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/deletar_email/{email_hash}")
async def api_deletar_email(email_hash: str):
    resultado = deletar_email(email_hash)
    if "Erro" in resultado:
        raise HTTPException(status_code=500, detail=resultado)
    return {"message": resultado}