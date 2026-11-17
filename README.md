# orion-platform

Ingestão dos dados da API Orion para o Postgres do Supabase, rodando em
GitHub Actions. A visualização é feita pelo `ews_monitor.html` (fora deste
repositório), que lê o `dados.json` gerado pelo `exportar_supabase.py`.

## Estrutura

```
.github/workflows/
    ingestao_15_min.yml   ingestão incremental, a cada 15 min
    alert_engine.yml      alertas por e-mail, a cada 15 min
    backfill.yml          recuperação de histórico, manual
ingestao/
    ingest_incremental.py  cadastro de devices/sensores + download de leituras
    backfill.py            recuperação de um período inteiro, sob demanda
    common.py              sessão HTTP, token e conexão com o banco
alert_engine.py           alertas TARP e offline
requirements.txt
sql/001_manutencao.sql    schema de referência e consultas de diagnóstico
```

## Secrets necessários

| Secret | Usado por |
|---|---|
| `API_KEY` | ingestão e backfill (chave da Orion) |
| `DATABASE_URL` | todos |
| `EMAIL_HOST`, `EMAIL_PORT`, `EMAIL_USER`, `EMAIL_PASSWORD`, `EMAIL_FROM` | alert_engine |

## Como funciona a ingestão

1. `GET /UserDevices` traz devices e canais. Só os 9 tipos listados em
   `TIPOS_VALIDOS` são gravados; o resto é contado e ignorado.
2. Para cada sensor, `sync_state.last_timestamp` guarda até onde já foi
   baixado. A janela recua 1 h para pegar leitura que chegou atrasada.
3. `GET /SensorData` é chamado em lotes de 50 sensores. Mais que isso faz
   a URL estourar e a Orion devolver 404.
4. O token é renovado a cada 8 min e em qualquer 401.
5. Qualquer lote que falhe faz o job terminar vermelho. Job verde significa
   que tudo foi baixado.

## Quando os dados pararem de atualizar

```sql
SELECT max(data_leitura) FROM leituras;
```

Se a data estiver velha, abra *Actions* e veja a última execução de
`Ingestão_Orion_(15min)`. Causas mais comuns: `API_KEY` revogada na Orion,
projeto do Supabase pausado, ou o agendamento desativado pelo GitHub após
60 dias sem commits.

Para recuperar um período grande, rode o workflow `Backfill histórico
(manual)` informando a data inicial.
