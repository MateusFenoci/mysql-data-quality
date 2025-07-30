# Git Flow Guide - Tupy Data Quality

## Visão Geral

Este projeto utiliza Git Flow para gerenciar releases, features e hotfixes de forma organizada e estruturada.

## Estrutura de Branches

### Branches Principais

- **`main`**: Branch de produção com código estável
- **`develop`**: Branch de desenvolvimento com features integradas

### Branches Temporárias

- **`feature/*`**: Desenvolvimento de novas funcionalidades
- **`release/*`**: Preparação de releases
- **`hotfix/*`**: Correções urgentes em produção
- **`bugfix/*`**: Correções de bugs no develop

## Workflow Detalhado

### 1. Iniciando uma Nova Feature

```bash
# Partindo do develop atualizado
git checkout develop
git pull origin develop

# Criando branch da feature
git checkout -b feature/nome-da-feature

# Exemplo: feature/add-postgres-connector
git checkout -b feature/add-postgres-connector
```

### 2. Trabalhando na Feature

**❌ EVITAR (Commitão Massivo):**
```bash
git add .
git commit -m "Add postgres connector with tests and docs"
```

**✅ RECOMENDADO (Commits Organizados):**
```bash
# 1. Adicionar estrutura base
git add src/data_quality/connectors/postgresql.py
git commit -m "feat(connectors): add PostgreSQL connector base structure

- Create PostgreSQLConnector class
- Implement connection interface
- Add basic configuration support"

# 2. Implementar funcionalidades específicas
git add src/data_quality/connectors/postgresql.py
git commit -m "feat(connectors): implement PostgreSQL query execution

- Add execute_query method with error handling
- Implement get_table_info for schema inspection
- Add connection pooling support"

# 3. Adicionar testes
git add tests/unit/connectors/test_postgresql.py
git commit -m "test(connectors): add comprehensive PostgreSQL connector tests

- Test connection establishment and failure cases
- Test query execution with various scenarios
- Add mock database interactions
- Achieve 95% test coverage"

# 4. Atualizar documentação
git add docs/connectors.md README.md
git commit -m "docs(connectors): document PostgreSQL connector usage

- Add connection string examples
- Document configuration options
- Add troubleshooting section
- Update main README with PostgreSQL support"

# 5. Atualizar dependencies se necessário
git add pyproject.toml poetry.lock
git commit -m "deps: add psycopg2 for PostgreSQL support

- Add psycopg2-binary ^2.9.0
- Update poetry.lock
- Add to dev dependencies for testing"
```

### 3. Convenção de Commits

Seguimos [Conventional Commits](https://www.conventionalcommits.org/):

**Formato:**
```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

**Tipos:**
- `feat`: Nova funcionalidade
- `fix`: Correção de bug
- `docs`: Mudanças na documentação
- `test`: Adição ou correção de testes
- `refactor`: Refatoração sem mudança de funcionalidade
- `perf`: Melhoria de performance
- `style`: Mudanças de formatação
- `ci`: Mudanças no CI/CD
- `chore`: Tarefas de manutenção

**Escopos:**
- `core`: Módulo core (orchestrator, analyzer, etc.)
- `validators`: Validadores de dados
- `connectors`: Conectores de banco
- `reports`: Sistema de relatórios
- `cli`: Interface de linha de comando
- `config`: Configurações
- `tests`: Testes (quando não específico)

### 4. Finalizando a Feature

```bash
# Push da feature
git push origin feature/add-postgres-connector

# Criar Pull Request para develop
gh pr create --title "feat(connectors): add PostgreSQL connector support" \
  --body "$(cat <<EOF
## 📋 Summary
- Add PostgreSQL connector with full functionality
- Implement connection pooling and error handling
- Add comprehensive test suite (95% coverage)
- Update documentation and examples

## 🧪 Test Plan
- [x] Unit tests passing
- [x] Integration tests with real PostgreSQL
- [x] Documentation examples validated
- [x] CLI integration tested

## 📚 Documentation
- Updated connector documentation
- Added configuration examples
- Included troubleshooting guide

🤖 Generated with [Claude Code](https://claude.ai/code)
EOF
)" --base develop
```

### 5. Release Process

```bash
# Criar branch de release
git checkout develop
git pull origin develop
git checkout -b release/1.2.0

# Finalizar preparação (bump version, changelog, etc.)
poetry version patch  # ou minor/major
git add pyproject.toml
git commit -m "chore(release): bump version to 1.2.0"

# Gerar changelog
git log --pretty=format:"- %s (%h)" v1.1.0..HEAD > CHANGELOG.md
git add CHANGELOG.md
git commit -m "docs(release): update changelog for v1.2.0"

# Merge para main e develop
git checkout main
git merge --no-ff release/1.2.0
git tag -a v1.2.0 -m "Release version 1.2.0"

git checkout develop
git merge --no-ff release/1.2.0

# Push e cleanup
git push origin main develop --tags
git branch -d release/1.2.0
git push origin --delete release/1.2.0
```

### 6. Hotfix Process

```bash
# Partir do main para hotfix crítico
git checkout main
git pull origin main
git checkout -b hotfix/fix-critical-bug

# Fazer correção específica
git add src/data_quality/core/orchestrator.py
git commit -m "fix(core): resolve memory leak in orchestrator

- Fix validation engine cleanup
- Add proper resource disposal
- Prevent accumulation of validators

Fixes #123"

# Merge para main e develop
git checkout main
git merge --no-ff hotfix/fix-critical-bug
git tag -a v1.2.1 -m "Hotfix version 1.2.1"

git checkout develop
git merge --no-ff hotfix/fix-critical-bug

git push origin main develop --tags
git branch -d hotfix/fix-critical-bug
```

## CI/CD Integration

### Branch Protection Rules

**Main Branch:**
- Require pull request reviews (2 reviewers)
- Require status checks to pass
- Require up-to-date branches
- No direct pushes allowed

**Develop Branch:**
- Require pull request reviews (1 reviewer)
- Require status checks to pass
- Allow admin pushes for hotfix merges

### Pipeline Triggers

- **Push to `develop`**: Deploy to development environment
- **Push to `main`**: Create release and deploy to production
- **Pull Requests**: Run full test suite and quality gates
- **Feature branches**: Run basic tests and linting

## Exemplos Práticos de Organização

### ❌ Commit Massivo (Evitar)

```bash
git add .
git commit -m "Add new report system with tests and fix bugs"
# Resultado: 47 files changed, 2,847 insertions(+), 156 deletions(-)
```

### ✅ Commits Organizados (Recomendado)

```bash
# Commit 1: Core structure
git add src/data_quality/core/report_manager.py
git commit -m "feat(core): add ReportManager base structure

- Create ReportManager abstract class
- Define report generation interface
- Implement dependency injection pattern"

# Commit 2: HTML report implementation
git add src/data_quality/reports/html_report.py
git commit -m "feat(reports): implement HTML report generator

- Add responsive HTML template
- Include CSS styling for professional appearance
- Support multiple validation result types
- Add severity-based color coding"

# Commit 3: JSON report implementation
git add src/data_quality/reports/json_report.py
git commit -m "feat(reports): implement JSON report generator

- Add machine-readable JSON format
- Support for all validation result fields
- Include metadata and timestamps
- Handle numpy type serialization"

# Commit 4: Tests for reports
git add tests/unit/core/test_report_manager.py tests/unit/reports/
git commit -m "test(reports): add comprehensive report system tests

- Test ReportManager with dependency injection
- Add HTML report generation tests
- Add JSON report serialization tests
- Achieve 85% coverage on reports module"

# Commit 5: CLI integration
git add src/data_quality/cli.py
git commit -m "feat(cli): integrate report system with analyze command

- Add --formats option for report type selection
- Add --separate-reports flag for individual files
- Update help documentation
- Maintain backward compatibility"

# Commit 6: Documentation
git add docs/reports.md docs/cli-usage.md
git commit -m "docs(reports): document report system usage

- Add report generation examples
- Document format options and outputs
- Include troubleshooting section
- Update CLI command reference"
```

## Quality Gates

### Pre-commit Hooks

```bash
# Install pre-commit
poetry add --group dev pre-commit
pre-commit install

# .pre-commit-config.yaml inclui:
# - black (formatting)
# - ruff (linting)
# - mypy (type checking)
# - pytest (basic tests)
```

### PR Requirements

- ✅ All tests passing (coverage ≥ 70%)
- ✅ Linting and formatting checks
- ✅ Type checking without errors
- ✅ Security scan clean
- ✅ Documentation updated
- ✅ Conventional commit format
- ✅ Branch naming follows Git Flow

## Comandos Úteis

```bash
# Ver commits organizados
git log --oneline --graph --decorate

# Ver estatísticas de um commit
git show --stat <commit-hash>

# Reescrever histórico (use com cuidado)
git rebase -i HEAD~3

# Ver branches remotas
git branch -r

# Limpar branches locais já mergeadas
git branch --merged | grep -v "main\|develop" | xargs -n 1 git branch -d
```

## Boas Práticas

1. **Commits Pequenos e Focados**: Cada commit deve representar uma mudança lógica
2. **Mensagens Descritivas**: Explique o "porquê", não apenas o "o que"
3. **Testes Sempre**: Cada feature deve incluir testes apropriados
4. **Documentação Atualizada**: Manter docs sincronizadas com código
5. **Review de Código**: Sempre revisar PRs antes do merge
6. **Pipeline Limpo**: Resolver todos os issues do CI/CD antes do merge

## Troubleshooting

### Branch fora de sincronização
```bash
git checkout develop
git pull origin develop
git checkout feature/sua-feature
git rebase develop  # ou git merge develop
```

### Resolver conflitos
```bash
git status
# Editar arquivos em conflito
git add arquivo-resolvido.py
git rebase --continue  # ou git merge --continue
```

### Desfazer commit local
```bash
git reset --soft HEAD~1  # Manter mudanças staged
git reset --hard HEAD~1  # Descartar mudanças (cuidado!)
```
