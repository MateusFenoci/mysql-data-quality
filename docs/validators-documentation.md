# Documentação de Validadores de Qualidade de Dados

## Visão Geral

Este documento fornece uma documentação abrangente para o sistema de validação de qualidade de dados, implementado seguindo os princípios **SOLID** e o Desenvolvimento Guiado por Testes (**TDD**) com o padrão Triple-A.

---
## Arquitetura

O sistema de validação é construído sobre uma arquitetura modular que promove a reutilização de código, a manutenibilidade e a extensibilidade:

```

src/data_quality/
├── validators/
│   ├── base.py           \# Classes base abstratas e tipos principais
│   ├── completeness.py   \# Validação de valores nulos/ausentes
│   ├── duplicates.py     \# Detecção de dados duplicados
│   ├── integrity.py      \# Verificação de integridade referencial
│   └── patterns.py       \# Validação de formatos e padrões
├── reports/
│   ├── base.py           \# Gerador de relatórios abstrato
│   ├── html\_report.py    \# Geração de relatórios em HTML
│   ├── json\_report.py    \# Geração de relatórios em JSON
│   └── summary\_report.py \# Relatórios resumidos em texto
└── cli.py                \# Interface de linha de comando

```

---
## Componentes Principais

### Classes Base (`validators/base.py`)

#### ValidationSeverity
Uma enumeração que define os níveis de severidade para os resultados da validação:
- `INFO`: Mensagens informativas
- `WARNING`: Problemas não críticos que devem ser revisados
- `ERROR`: Problemas críticos de qualidade de dados
- `CRITICAL`: Problemas graves que exigem atenção imediata

#### ValidationResult
Uma *data class* que representa o resultado de uma verificação de validação:
```python
@dataclass
class ValidationResult:
    rule_name: str              # Nome da regra de validação
    table_name: str             # Tabela que está sendo validada
    column_name: Optional[str]  # Nome da coluna (se aplicável)
    severity: ValidationSeverity# Nível de severidade
    passed: bool                # Se a validação passou
    message: str                # Mensagem legível por humanos
    details: Dict[str, Any]     # Detalhes adicionais da validação
    timestamp: datetime         # Quando a validação foi realizada
    affected_rows: int = 0      # Número de linhas afetadas
    total_rows: int = 0         # Número total de linhas verificadas
```

#### ValidationRule

Classe de configuração para as regras de validação:

```python
@dataclass
class ValidationRule:
    name: str                   # Identificador da regra
    description: str            # Descrição legível por humanos
    severity: ValidationSeverity# Nível de severidade para falhas
    parameters: Dict[str, Any]  # Parâmetros específicos da regra
```

#### DataQualityValidator (Classe Base Abstrata)

Classe base abstrata que implementa o padrão *Template Method*. Todos os validadores devem herdar desta classe e implementar o método `validate_table`.

#### ValidationEngine

Orquestra múltiplos validadores e fornece uma interface unificada para executar as validações.

-----

## Implementações dos Validadores

### 1\. Validador de Completude (`validators/completeness.py`)

**Objetivo**: Valida a completude dos dados, verificando a presença de valores nulos ou ausentes.

**Regras Padrão**:

  - `default_completeness`: Exige ≥95% de completude para todas as colunas.

**Principais Características**:

  - Limiares de completude configuráveis.
  - Lida corretamente com os tipos de dados nulos do pandas.
  - Fornece taxas de completude detalhadas.

**Exemplo de Uso**:

```python
validator = CompletenessValidator()
results = validator.validate_table(data, "clientes")
```

**Exemplo de Configuração de Regra**:

```python
rule = ValidationRule(
    name="verificacao_alta_completude",
    description="Exigir 99% de completude para campos críticos",
    severity=ValidationSeverity.ERROR,
    parameters={"threshold": 0.99, "columns": ["id_cliente", "email"]}
)
```

### 2\. Validador de Duplicatas (`validators/duplicates.py`)

**Objetivo**: Detecta dados duplicados em colunas individuais ou chaves compostas.

**Regras Padrão**:

  - `default_uniqueness`: Garante que não haja valores duplicados em nenhuma coluna.

**Principais Características**:

  - Detecção de duplicatas em uma única coluna.
  - Detecção de duplicatas em chaves compostas.
  - Tratamento de nulos configurável (parâmetro `ignore_nulls`).
  - Apresenta amostras de valores duplicados nos resultados.

**Exemplo de Uso**:

```python
validator = DuplicatesValidator()
results = validator.validate_table(data, "produtos")
```

**Exemplo de Configuração de Regra**:

```python
rule = ValidationRule(
    name="email_cliente_unico",
    description="Os e-mails dos clientes devem ser únicos",
    severity=ValidationSeverity.CRITICAL,
    parameters={"columns": ["email"], "ignore_nulls": True}
)
```

### 3\. Validador de Integridade (`validators/integrity.py`)

**Objetivo**: Valida a integridade referencial e os relacionamentos de chave estrangeira.

**Principais Características**:

  - Validação de chave estrangeira com consultas ao banco de dados.
  - Suporte a chaves estrangeiras compostas.
  - Suporte a tabelas com autorreferenciamento.
  - Integração configurável com conectores de banco de dados.

**Exemplo de Uso**:

```python
validator = IntegrityValidator()
# Requer configuração manual de regras para relacionamentos específicos
```

**Exemplo de Configuração de Regra**:

```python
rule = ValidationRule(
    name="fk_pedidos_cliente",
    description="Todos os customer_ids dos pedidos devem existir na tabela de clientes",
    severity=ValidationSeverity.ERROR,
    parameters={
        "parent_table": "clientes",
        "parent_columns": ["id"],
        "child_columns": ["id_cliente"]
    }
)
```

### 4\. Validador de Padrões (`validators/patterns.py`)

**Objetivo**: Valida formatos e padrões de dados, especialmente identificadores fiscais brasileiros.

**Regras Padrão**:

  - `default_pattern_check`: Detecta padrões automaticamente com base nos nomes das colunas.

**Padrões Suportados**:

  - **CNPJ**: Cadastro Nacional da Pessoa Jurídica com validação de dígito verificador.
  - **CPF**: Cadastro de Pessoas Físicas com validação de dígito verificador.
  - **Email**: Validação de formato de e-mail compatível com RFC.
  - **Telefone**: Padrões de números de telefone brasileiros.
  - **CEP**: Formato de Código de Endereçamento Postal brasileiro.
  - **Regex Personalizado**: Expressões regulares definidas pelo usuário.

**Principais Características**:

  - Detecção automática de padrões com base nos nomes das colunas.
  - Algoritmos matematicamente corretos para validação de CNPJ/CPF.
  - Tipos de padrões e limiares configuráveis.
  - Suporte a padrões de regex personalizados.

**Exemplo de Uso**:

```python
validator = PatternsValidator()
results = validator.validate_table(data, "empresas")
```

**Exemplo de Configuração de Regra**:

```python
rule = ValidationRule(
    name="verificacao_formato_cnpj",
    description="O CNPJ da empresa deve ter um formato válido",
    severity=ValidationSeverity.ERROR,
    parameters={
        "pattern_type": "cnpj",
        "columns": ["cnpj_empresa"],
        "threshold": 0.95
    }
)
```

-----

## Sistema de Relatórios

### Tipos de Relatório

#### 1\. Relatórios HTML (`reports/html_report.py`)

  - **Formato**: HTML interativo com estilo CSS.
  - **Recursos**:
      - Design responsivo.
      - Código de cores baseado na severidade.
      - Detalhamento das validações.
      - Cartões com estatísticas resumidas.
  - **Caso de Uso**: Relatórios legíveis para stakeholders.

#### 2\. Relatórios JSON (`reports/json_report.py`)

  - **Formato**: JSON legível por máquina.
  - **Recursos**:
      - Serialização completa dos resultados.
      - Inclusão de metadados.
      - Pronto para integração com APIs.
  - **Caso de Uso**: Integração de sistemas e processamento automatizado.

#### 3\. Relatórios Resumidos (`reports/summary_report.py`)

  - **Formato**: Texto simples com emojis e formatação.
  - **Recursos**:
      - Pontuações de qualidade concisas.
      - Identificação dos principais problemas.
      - Recomendações práticas.
      - Classificação da pontuação de qualidade (EXCELENTE, BOM, RAZOÁVEL, RUIM, CRÍTICO).
  - **Caso de Uso**: Avaliações rápidas de qualidade e alertas.

### Geração de Relatórios

Todos os relatórios incluem:

  - Métricas gerais de qualidade (total de verificações, taxa de aprovação, taxa de sucesso).
  - Detalhamento por validador (desempenho por tipo de validador).
  - Detalhamento por severidade (problemas por nível de severidade).
  - Resultados detalhados da validação.
  - Metadados (tamanho da amostra, informações da tabela, validadores usados).

-----

## Análise de Relacionamentos do Banco de Dados

Esta seção fornece orientações gerais para analisar relacionamentos de banco de dados usando o framework de validação:

### Padrões Comuns de Entidades de Negócio

#### 1\. Tabelas de Dados Mestres

  - **Chaves Primárias**: Identificadores únicos para entidades de negócio.
  - **Identificadores de Negócio**: Chaves naturais como IDs fiscais, códigos, etc.
  - **Atributos Comuns**: Nomes, descrições, relacionamentos hierárquicos.
  - **Considerações de Qualidade**:
      - Monitorar duplicatas de identificadores de negócio.
      - Validar o formato de IDs fiscais e outros identificadores regulados.
      - Verificar a consistência dos relacionamentos hierárquicos.

#### 2\. Tabelas de Clientes

  - **Chave Primária**: Identificador único do cliente.
  - **Chaves Estrangeiras**: Links para entidades organizacionais ou geográficas.
  - **Atributos Comuns**: Informações de contato, preferências, dados demográficos.
  - **Considerações de Qualidade**:
      - Validar formatos das informações de contato.
      - Monitorar a completude de dados críticos dos clientes.
      - Verificar a integridade referencial com tabelas relacionadas.

#### 3\. Tabelas de Transações

  - **Volume**: Frequentemente de alto volume, com milhões de registros.
  - **Relacionamentos**: Múltiplas chaves estrangeiras para dados mestres.
  - **Preocupações de Qualidade**: Requer amostragem para validação de grandes conjuntos de dados.

### Recomendações Gerais para Melhoria da Qualidade dos Dados

1.  **Processo de Deduplicação**: Implementar deduplicação automatizada para dados mestres.
2.  **Integridade Referencial**: Estabelecer e manter relacionamentos de chave estrangeira.
3.  **Padronização de Dados**: Padronizar convenções de nomenclatura e formatos.
4.  **Monitoramento de Completude**: Implementar verificações de completude automatizadas.
5.  **Validação de Formato**: Garantir que identificadores regulatórios sigam os formatos corretos.

-----

## Exemplos de Uso

### Interface de Linha de Comando

```bash
# Validação básica
poetry run python -m data_quality.cli validate empresas

# Com validadores específicos
poetry run python -m data_quality.cli validate clientes --validators completeness duplicates

# Com relatórios
poetry run python -m data_quality.cli validate empresas --report-format html json summary

# Com amostragem para tabelas grandes
poetry run python -m data_quality.cli validate transacoes --sample-size 50000 --report-format summary
```

### Uso Programático

```python
from data_quality.validators import ValidationEngine, CompletenessValidator
from data_quality.reports import HTMLReportGenerator

# Inicializa o motor de validação
engine = ValidationEngine()
engine.register_validator(CompletenessValidator())

# Executa as validações
results = engine.validate_data(dataframe, "nome_tabela")

# Gera relatórios
html_generator = HTMLReportGenerator("relatorios/")
report_path = html_generator.generate_report(results, "nome_tabela")
```

-----

## Estratégia de Testes

O sistema de validação segue o Desenvolvimento Guiado por Testes (TDD) com o padrão Triple-A:

### Estrutura dos Testes

  - **Arrange (Organizar)**: Configurar dados de teste e instâncias de validadores.
  - **Act (Agir)**: Executar os métodos de validação.
  - **Assert (Verificar)**: Verificar os resultados esperados.

### Cobertura de Testes

  - **Testes Unitários**: 67 testes passando, cobrindo todos os validadores.
  - **Testes de Integração**: Conectividade com banco de dados e validação de dados reais.
  - **Casos Extremos**: Tratamento de nulos, conjuntos de dados vazios, condições de limite.

### Arquivos de Teste

  - `tests/unit/validators/test_completeness.py`: Testes do validador de completude.
  - `tests/unit/validators/test_duplicates.py`: Testes do validador de duplicatas.
  - `tests/unit/validators/test_integrity.py`: Testes do validador de integridade.
  - `tests/unit/validators/test_patterns.py`: Testes do validador de padrões.

-----

## Implementação dos Princípios SOLID

### Princípio da Responsabilidade Única (SRP)

  - Cada validador tem uma responsabilidade única e bem definida.
  - Classes separadas para cada tipo de validação (completude, duplicatas, etc.).

### Princípio Aberto/Fechado (OCP)

  - As classes base fornecem pontos de extensão para novos validadores.
  - Os validadores existentes podem ser estendidos sem modificação.

### Princípio da Substituição de Liskov (LSP)

  - Todos os validadores implementam a mesma interface e podem ser usados de forma intercambiável.
  - O `ValidationEngine` funciona com qualquer implementação de `DataQualityValidator`.

### Princípio da Segregação de Interfaces (ISP)

  - Interfaces limpas e focadas para validação e relatórios.
  - Os validadores dependem apenas das interfaces que utilizam.

### Princípio da Inversão de Dependência (DIP)

  - O `ValidationEngine` de alto nível depende de abstrações, não de implementações concretas.
  - Os conectores de banco de dados são injetados como dependências.

-----

## Considerações de Desempenho

### Manuseio de Grandes Conjuntos de Dados

  - **Amostragem**: Amostragem automática para tabelas com mais de 10.000 linhas.
  - **Gerenciamento de Memória**: Operações eficientes com pandas.
  - **Conexões com Banco de Dados**: Pool de conexões e limpeza adequada.

### Estratégias de Otimização

  - **Validação Seletiva**: Executar apenas os validadores necessários.
  - **Processamento em Lote**: Processar dados em blocos gerenciáveis.
  - **Cache**: Armazenar em cache os resultados da validação para análises repetidas.

-----

## Configuração e Extensibilidade

### Adicionando Novos Validadores

1.  Herde de `DataQualityValidator`.
2.  Implemente o método `validate_table`.
3.  Defina as regras padrão em `__init__`.
4.  Adicione testes unitários abrangentes.

### Regras de Validação Personalizadas

  - Crie instâncias de `ValidationRule` com parâmetros personalizados.
  - Registre as regras com os validadores apropriados.
  - Configure os níveis de severidade e os limiares.

### Configuração do Ambiente

  - Conexões com o banco de dados via arquivo `.env`.
  - Limiares de amostragem configuráveis.
  - Especificações do diretório de saída.
