#!/usr/bin/env python3
"""Script para testar conexão com o banco de dados."""

import sys
from pathlib import Path

# Adiciona o src ao path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_quality.config import load_config
from data_quality.connectors.factory import DatabaseConnectorFactory


def test_database_connection():
    """Testa a conexão com o banco de dados."""
    try:
        # Carrega configuração
        config = load_config()
        db_config = config["database"]

        print(f"🔌 Testando conexão com {db_config.driver}...")
        print(f"   Host: {db_config.host}:{db_config.port}")
        print(f"   Database: {db_config.name}")
        print(f"   User: {db_config.user}")

        # Cria connector
        connector = DatabaseConnectorFactory.create_connector(
            db_config.connection_string, db_config.driver
        )

        # Testa conexão
        print("\n⏳ Conectando...")
        print(f"   Connection String: {db_config.connection_string}")

        try:
            connector.connect()
        except Exception as e:
            print(f"❌ Erro na conexão: {e}")
            return False

        if connector.test_connection():
            print("✅ Conexão estabelecida com sucesso!")

            # Lista algumas tabelas
            print("\n📋 Listando tabelas disponíveis...")
            tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            ORDER BY table_name
            LIMIT 10
            """

            tables_df = connector.execute_query(tables_query)
            print(f"   Encontradas {len(tables_df)} tabelas:")

            for idx, row in tables_df.iterrows():
                table_name = row["table_name"]
                print(f"   - {table_name}")

                # Para a primeira tabela, mostra as colunas
                if idx == 0:
                    print(f"\n🏗️  Estrutura da tabela '{table_name}':")
                    try:
                        columns_info = connector.get_table_info(table_name)
                        for col in columns_info:
                            col_name = col.get("column_name", "N/A")
                            col_type = col.get("data_type", "N/A")
                            nullable = col.get("is_nullable", "N/A")
                            print(
                                f"      {col_name} ({col_type}) - Nullable: {nullable}"
                            )
                    except Exception as e:
                        print(f"      ❌ Erro ao obter colunas: {e}")

            # Teste de contagem em uma tabela
            if len(tables_df) > 0:
                first_table = tables_df.iloc[0]["table_name"]
                try:
                    count = connector.get_table_count(first_table)
                    print(f"\n📊 Registros na tabela '{first_table}': {count:,}")
                except Exception as e:
                    print(f"\n❌ Erro ao contar registros: {e}")

        else:
            print("❌ Falha na conexão!")

        connector.disconnect()
        print("\n🔌 Conexão encerrada.")

    except Exception as e:
        print(f"❌ Erro: {e}")
        return False

    return True


if __name__ == "__main__":
    print("🚀 Teste de Conexão - Data Quality Tool")
    print("=" * 50)

    success = test_database_connection()

    if success:
        print("\n✅ Teste concluído com sucesso!")
    else:
        print("\n❌ Teste falhou!")
        sys.exit(1)
