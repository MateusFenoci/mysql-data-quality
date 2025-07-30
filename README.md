# Data Quality Tool

[![Version](https://img.shields.io/badge/version-0.4.0-blue)](.)
[![Build Status](https://img.shields.io/github/actions/workflow/status/MateusFenoci/mysql-data-quality/ci.yml?branch=main)](https://github.com/MateusFenoci/mysql-data-quality/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-92%25-brightgreen)](.)
[![Tests](https://img.shields.io/badge/tests-17%20passed-brightgreen)](.)
[![Python](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Code Quality](https://img.shields.io/badge/code%20quality-A-green)](.)

🔍 Ferramenta para validação e análise de qualidade de dados em bancos de dados, com foco em MariaDB/MySQL.

## 🚀 Quick Start

### 1. Setup Inicial

```bash
# Configuração completa do ambiente
./scripts/dev.sh setup

# Ou usando Python diretamente
python scripts/setup.py
```

### 2. Comandos Principais

```bash
# Testar conexão com banco
./scripts/dev.sh connect

# Listar tabelas
./scripts/dev.sh tables

# Descrever estrutura de uma tabela
./scripts/dev.sh describe nome_da_tabela

# Executar validações de qualidade
./scripts/dev.sh validate
```

## 🏗️ Estrutura do Projeto

```
data-quality/
├── src/data_quality/           # Código principal
│   ├── __init__.py
│   ├── cli.py                  # Interface CLI
│   ├── config.py               # Configurações
│   ├── connectors/             # Conectores de banco
│   │   ├── base.py
│   │   ├── mysql.py
│   │   ├── postgresql.py
│   │   └── factory.py
│   ├── core/                   # Lógica principal
│   ├── validators/             # Validadores de dados
│   └── reports/                # Geração de relatórios
├── tests/                      # Testes
│   ├── unit/
│   └── integration/
├── scripts/                    # Scripts utilitários
│   ├── setup.py               # Setup do ambiente
│   ├── setup.sh               # Setup (bash)
│   └── dev.sh                 # Comandos de desenvolvimento
├── config/                     # Configurações
├── .github/workflows/          # CI/CD
└── docs/                       # Documentação
```

## ⚙️ Configuração

### Banco de Dados

Configure o arquivo `.env` com suas credenciais:

```env
# Database Configuration
DB_HOST=your-host
DB_PORT=3306
DB_NAME=your-database
DB_USER=your-username
DB_PASSWORD=your-password
DB_DRIVER=mysql

# Application Settings
LOG_LEVEL=INFO
REPORTS_OUTPUT_DIR=./reports
MAX_CONNECTIONS=10
ENVIRONMENT=development
SECRET_KEY=your-secret-key
```

### Bancos Suportados

- ✅ **MySQL/MariaDB** (`DB_DRIVER=mysql`)

## 🛠️ Desenvolvimento

### Scripts Disponíveis

```bash
# Setup e Instalação
./scripts/dev.sh setup       # Setup completo
./scripts/dev.sh install     # Instalar dependências

# Desenvolvimento
./scripts/dev.sh test        # Executar testes
./scripts/dev.sh test-cov    # Testes com cobertura
./scripts/dev.sh lint        # Linting
./scripts/dev.sh format      # Formatação de código
./scripts/dev.sh type-check  # Verificação de tipos
./scripts/dev.sh security    # Verificações de segurança
./scripts/dev.sh clean       # Limpeza de arquivos

# Banco de Dados
./scripts/dev.sh connect     # Testar conexão
./scripts/dev.sh tables      # Listar tabelas
./scripts/dev.sh describe <table> # Descrever tabela
./scripts/dev.sh validate    # Executar validações

# Utilitários
./scripts/dev.sh shell       # Poetry shell
./scripts/dev.sh help        # Ajuda
```

### Usando Poetry Diretamente

```bash
# Instalar dependências
poetry install

# CLI da aplicação
poetry run data-quality --help
poetry run data-quality test-connection
poetry run data-quality list-tables
poetry run data-quality describe-table nome_da_tabela

# Desenvolvimento
poetry run pytest                    # Testes
poetry run black .                   # Formatação
poetry run flake8 .                  # Linting
poetry run mypy src                  # Type checking
```

### Usando Makefile

```bash
make help           # Ajuda
make setup          # Setup completo
make test           # Testes
make lint           # Linting
make format         # Formatação
make type-check     # Verificação de tipos
make security       # Verificações de segurança
make clean          # Limpeza
```

## 🔍 Funcionalidades

### Análises de Qualidade de Dados

- **Volumetria**: Contagem de registros por tabela
- **Completude**: Verificação de campos nulos/vazios
- **Consistência**: Validação de tipos de dados
- **Integridade Referencial**: Verificação de foreign keys
- **Duplicatas**: Identificação de registros duplicados
- **Outliers**: Detecção de valores atípicos
- **Padrões**: Validação de formatos (CPF, CNPJ, email, etc.)

### Relatórios

- Relatórios em HTML, PDF e Excel
- Dashboards interativos
- Métricas de qualidade
- Histórico de validações

## 🏭 Produção

### CI/CD

O projeto inclui GitHub Actions para:

- ✅ Testes automatizados (Python 3.10, 3.11, 3.12)
- ✅ Linting e formatação (Black, Ruff)
- ✅ Type checking (MyPy)
- ✅ Verificações de segurança (Bandit)
- ✅ Code coverage (>90% obrigatório)
- ✅ Git Flow validation
- ✅ **Versionamento automático** (Semantic Versioning)
- ✅ **Shields dinâmicos** (atualizados automaticamente)
- ✅ **Releases automatizados** com changelog

### Versionamento Automático

O projeto usa **Semantic Versioning** com base nos commits:

```bash
# Tipos de commit que afetam a versão:
feat: nova funcionalidade     → versão minor (1.0.0 → 1.1.0)
fix: correção de bug         → versão patch (1.0.0 → 1.0.1)
BREAKING CHANGE: mudança     → versão major (1.0.0 → 2.0.0)

# Versionamento manual local:
./scripts/version.sh patch   # Para correções
./scripts/version.sh minor   # Para novas features
./scripts/version.sh major   # Para breaking changes
```

### Segurança

- Pre-commit hooks configurados
- Validação de secrets com GitGuardian
- Análise de vulnerabilidades com Bandit
- Configuração segura de variáveis de ambiente

## 📚 Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -m 'Add nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Abra um Pull Request

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para detalhes.

## 🤝 Suporte

Para suporte e dúvidas:

- 📧 Email: fenocimateus@gmail.com
- 🐛 Issues: [Issues Internas](https://github.com/MateusFenoci/mysql-data-quality/issues)
- 📖 Documentação: [Docs](docs/)
