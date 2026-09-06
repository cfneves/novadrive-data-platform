if (-not (Test-Path ".env")) {
    Write-Error "Arquivo .env nao encontrado. Copie .env.example para .env e preencha as credenciais primeiro."
    exit 1
}

Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim())
    }
}

Write-Host "OK: variaveis de .env carregadas nesta sessao do PowerShell." -ForegroundColor Green