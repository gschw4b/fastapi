# Projeto de Processamento de E-mails com Anexos XLSX e Conversão para CSV

Este projeto tem como objetivo baixar e-mails não lidos com anexos `.xlsx`, converter esses arquivos para `.csv` e enviá-los de volta para o remetente.

## Funcionalidades

- Conecta-se a um servidor de e-mails IMAP para buscar e-mails não lidos.
- Baixa arquivos `.xlsx` anexados.
- Converte os arquivos `.xlsx` para `.csv`.
- Envia de volta os arquivos convertidos como anexos por e-mail usando SMTP.

## Tecnologias Utilizadas

- **FastAPI**: Framework para a criação da API REST.
- **Pandas**: Biblioteca para manipulação de dados e conversão de arquivos `.xlsx` para `.csv`.
- **IMAP**: Para conectar e buscar e-mails não lidos.
- **SMTP**: Para enviar e-mails com anexos.
- **Python 3.12.3**