# Teste Técnico – PySpark + Arquitetura Medalhão (BeFly)

## 📌 Visão Geral
Este projeto implementa um mini-pipeline de dados utilizando **PySpark**, aplicando a arquitetura **Medalhão (Bronze, Silver, Gold)**. O objetivo é processar dados de reservas de hotéis (*Hotel Booking Demand*), realizando limpeza, enriquecimento e criando visões analíticas agregadas.

## 🚀 Como Executar

### Pré-requisitos
- Python 3.8+ instalado.
- Java 8 ou 11 (necessário para rodar o Spark localmente).

### Passos
1. Instale as dependências:
```bash
pip install -r requirements.txt
```

2. Adicione os dados brutos:
Coloque os arquivos CSV extraídos do dataset na pasta `data/raw/`:
- `hotel_bookings.csv`
- `country_metadata.csv`
- `hotel_metadata.csv`

3. Execute o pipeline:
Na raiz do projeto, rode:
```bash
python src/main.py
```
*Ou, se preferir usar o spark-submit:*
```bash
spark-submit src/main.py
```

## 🛠️ Transformações e Camadas

- **Bronze:** Leitura dos CSVs com tratamento específico para a string `"NULL"` (convertida para `null` verdadeiro). Salvamento em Parquet com particionamento por ano no arquivo principal.
- **Silver:** Limpeza de nulos (`children`, `country`), conversão e criação da data `arrival_date`, criação de colunas derivadas (`total_nights`, `total_guests`, `revenue`) e join com tabelas de metadados geográficos e de hotel. Foram filtrados registros sem hóspedes e com ADR (Average Daily Rate) negativo. O DataFrame final foi salvo em Parquet, particionado por ano e mês.
- **Gold:** Criação de agregações.
  - **Visão 1:** Receita, ocupação e taxa de cancelamento agrupadas por hotel e mês.
  - **Visão 2:** Top 20 países que mais geram receita em reservas efetivadas.

## ☁️ Visão Arquitetural para Nuvem (AWS / Databricks)
Caso este pipeline fosse migrado para produção na nuvem da AWS, adotaríamos uma arquitetura de **Lakehouse**. O desenho seria o seguinte:

1. **Armazenamento (Storage):** Os dados não ficariam em um disco local, mas no **Amazon S3**. Criaríamos buckets separados (ou prefixos) para refletir as camadas lógicas: `s3://befly-datalake-bronze`, `silver` e `gold`.
2. **Formato de Dados:** Em vez de Parquet puro, usaríamos um formato de tabela transacional como **Delta Lake** ou **Apache Iceberg**. Isso nos daria recursos de *Time Travel*, operações UPSERT (MERGE) para lidar com atualizações de status de reservas (como cancelamentos tardios) e conformidade ACID.
3. **Processamento (Compute):** O código PySpark atual poderia ser executado no **AWS EMR (Serverless)**, no **AWS Glue** ou no próprio **Databricks** hospedado na AWS. A vantagem do Databricks é a otimização nativa do engine Spark e a forte integração com Delta Lake.
4. **Orquestração e Agendamento:** Usaríamos o **Apache Airflow** (via Amazon MWAA) ou **AWS Step Functions**. Teríamos uma DAG com dependências claras: *Task Ingestão (Bronze) -> Task Limpeza (Silver) -> Task Agregação (Gold)*.
5. **Governança e Consumo:** Utilizaríamos o **AWS Glue Data Catalog** para mapear os schemas do S3. Assim, times de Negócio e BI poderiam usar o **Amazon Athena** (ou Databricks SQL) para consultar a camada Gold via SQL puro e plugar ferramentas como PowerBI diretamente nessas tabelas agregadas.
