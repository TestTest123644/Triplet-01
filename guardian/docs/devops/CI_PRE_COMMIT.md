# CI and Pre-commit — Guardian Layer

## 1. Purpose

The project uses pre-commit hooks to check changes locally and GitHub Actions to validate Pull Requests before they are merged.

## 2. Pre-commit Hooks

The configuration is defined in `.pre-commit-config.yaml`.

- **Ruff format:** Checks whether Python code follows the expected formatting.
- **Ruff lint:** Detects Python code quality issues.
- **Tests:** Runs the project tests using pytest.
- **Commitizen:** Checks commit messages against the configured commit conventions.
- **Branch check:** Checks branch names according to the rules configured by the branch-check hook.

### Install the hooks

```bash
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg
```

### Run all hooks manually

```bash
uv run pre-commit run --all-files
```

## 3. CI Pipeline

The workflow is defined in `.github/workflows/ci.yml`.

It runs on Pull Requests and performs the following checks:

1. Checks out the repository.
2. Installs Python and project dependencies.
3. Checks Python code quality with Ruff.
4. Checks Python types with basedpyright.
5. Runs tests using pytest.
6. Searches for Dockerfiles and attempts to build Docker images from them.

If a Python check, test, or Docker build fails, the CI job fails.

If no Dockerfile is found, the Docker build step is skipped and an informational message is displayed.

## 4. Docker Compose

The `docker-compose.yml` file currently uses the prebuilt image `apache/kafka:3.8.0` for its Kafka services.

There are currently no Dockerfiles in the repository. Docker build checks will run automatically when Dockerfiles are added.

## 5. Checks Before Opening a Pull Request

Run the following commands locally:

```bash
uv run pre-commit run --all-files
git diff --check
git status
```

Also verify that the GitHub Actions workflow succeeds before merging the Pull Request.
