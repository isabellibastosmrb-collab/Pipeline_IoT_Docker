from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# Constantes do projeto

CSV_PATH = Path("data/IOT-temp.csv")                                            # Caminho do arquivo CSV contendo as leituras de temperatura
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@localhost:5433/iot_db"  # String de conexão com o banco PostgreSQL
TABLE_NAME = "iot_temperature_readings"                                         # Nome da tabela de destino no banco
SQL_DIR = Path("sql")                                                           # Diretório onde estão os scripts SQL do projeto               


# Função para carregar e limpar o CSV
def load_csv(csv_path: Path) -> pd.DataFrame:
    """
    Lê o arquivo CSV de leituras IoT, realiza limpeza e padronização
    dos dados e retorna um DataFrame pronto para inserção no banco.
    """

    # Leitura do CSV
    df = pd.read_csv(csv_path)

    print(f"Lendo arquivo: {csv_path.resolve()}")
    print(f"Total bruto no CSV: {len(df)}")

    # Padroniza nomes das colunas
    df.columns = df.columns.str.strip().str.lower()

    # Renomeia colunas problemáticas
    df = df.rename(columns={
        "room_id/id": "room_id",
        "out/in": "location_type"
    })

    # Conversões de tipo e limpeza de espaços
    df["id"] = df["id"].astype(str).str.strip()
    df["room_id"] = df["room_id"].astype(str).str.strip()
    df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce")
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df["location_type"] = df["location_type"].astype(str).str.strip().str.lower()

    df = df.dropna(subset=["id", "noted_date", "temp", "location_type"])  # Remove registros inválidos
    df = df[df["location_type"].isin(["in", "out"])]                      # Mantém apenas registros válidos de localização
    df = df.drop_duplicates(subset=["id"])                                # Remove duplicidades pelo ID

    print(f"Registros após limpeza: {len(df)}")

    return df


# Executa grupos de scripts SQL
def run_sql_group(engine, prefix: str) -> None:
    """
    Executa todos os arquivos SQL do diretório 'sql' que possuem
    determinado prefixo.

    Exemplo:
    01_create_table.sql
    02_create_views.sql
    """

    # Busca arquivos SQL ordenados pelo prefixo
    files = sorted(SQL_DIR.glob(f"{prefix}*.sql"))

    for file in files:
        print(f"Executando {file.name}")
        sql = file.read_text(encoding="utf-8")

        # Executa o script dentro de uma transação
        with engine.begin() as connection:
            connection.execute(text(sql))


# Limpa a tabela antes da carga
def truncate_table(engine) -> None:
    """
    Remove todos os registros da tabela destino antes
    de inserir novos dados (carga completa).
    """
    
    with engine.begin() as connection:
        connection.execute(text(f"TRUNCATE TABLE {TABLE_NAME};"))


# Insere os dados no banco
def insert_data(df: pd.DataFrame, engine) -> None:
    """
    Insere os dados do DataFrame na tabela PostgreSQL
    utilizando inserções em lote para melhor performance.
    """
    
    df.to_sql(
        TABLE_NAME,
        con=engine,
        if_exists="append",   # adiciona dados sem recriar tabela
        index=False,
        method="multi",       # inserção em lote
        chunksize=5000        # tamanho do lote
    )


# Função principal do pipeline
def main() -> None:
    """
    Orquestra todo o processo ETL:
    1. Conecta ao banco
    2. Carrega e limpa o CSV
    3. Executa scripts de criação de tabela
    4. Limpa a tabela
    5. Insere os dados
    6. Executa scripts de criação de views
    """
    
    engine = create_engine(DATABASE_URL)   # Cria engine de conexão com o banco
    df = load_csv(CSV_PATH)                # Carrega e prepara os dados
    
    run_sql_group(engine, "01_")           # Executa scripts iniciais (ex: criação de tabelas)
    truncate_table(engine)                 # Limpa tabela antes da carga
    insert_data(df, engine)                # Insere os dados no banco
    run_sql_group(engine, "02_")           # Executa scripts finais (ex: criação de views)

    print(f"Carga finalizada com {len(df)} registros.")


# Ponto de entrada do script
if __name__ == "__main__":
    main()