# tasks.ps1 — Windows-аналог Makefile. Запуск: .\tasks.ps1 <task>

param([Parameter(Position = 0)][string]$Task = "help")

function Show-Help {
    Write-Host "Usage: .\tasks.ps1 <task>"
    Write-Host ""
    Write-Host "Tasks:"
    Write-Host "  install     uv sync"
    Write-Host "  up          docker compose up -d (topics created automatically)"
    Write-Host "  down        docker compose down -v"
    Write-Host "  logs        docker compose logs -f"
    Write-Host "  ps          docker compose ps"
    Write-Host "  topics      list kafka topics"
    Write-Host "  lint        ruff check"
    Write-Host "  fmt         ruff format"
    Write-Host "  typecheck   mypy"
    Write-Host "  test        pytest"
}

switch ($Task) {
    "install"   { uv sync }
    "up"        {
        docker compose up -d
        Write-Host "Waiting for kafka-init..."
        Start-Sleep -Seconds 5
        docker compose ps kafka-init | Out-String | Write-Host
        Write-Host "Ready: kafka:29092, schema-registry:8081, kafka-ui:8090, postgres:5432, redis:6379"
    }
    "down"      { docker compose down -v }
    "logs"      { docker compose logs -f }
    "ps"        { docker compose ps }
    "topics"    { docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list }
    "lint"      { uv run ruff check . }
    "fmt"       { uv run ruff format . }
    "typecheck" { uv run mypy . }
    "test"      { uv run pytest }
    default     { Show-Help }
}
