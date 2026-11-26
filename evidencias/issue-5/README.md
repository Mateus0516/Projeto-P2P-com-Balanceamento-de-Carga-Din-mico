# Issue #5 — validação local de telemetria

Execução de 26/11/2026, das 17h18min20s às 17h18min40s, horário de São Paulo. O Collector de referência do professor aceitou seis mensagens TCP/NDJSON em loopback, três consecutivas por Master, com intervalos de aproximadamente 10 segundos e zero rejeições. Cada Master começou com dois workers registrados.

O [roteiro de captura](capturar.py) sobe os handlers reais dos dois Masters, quatro sessões do worker do projeto e o Collector oficial. Ele conduz `request_help` e `confirm_loan` pelo TCP para manter a oferta aberta até a primeira amostra. O roteiro usa a rotina de produção para espelhar o commit no destino e aciona a liberação após a segunda amostra. Os workers executam o redirecionamento, o registro e o retorno. Os Masters emitem os relatórios pelo loop periódico de produção. Todas as conexões da execução apontam para `127.0.0.1`.

| Uptime | Origem `grupo_07_master_01` | Destino `grupo_07_master_02` |
| --- | --- | --- |
| 10 s | `workers_reserved=1`; `offered` | `workers_received=0`; `loans=[]` |
| 20 s | `workers_lent=1`; `committed` | `workers_received=1`; `active` |
| 30 s | `workers_reserved=0`; `workers_lent=0`; `loans=[]` | `workers_received=0`; `loans=[]` |

O empréstimo `204ef95c-2295-49cc-a897-73fc89ded7c2` liga as amostras aos registros de negociação, redirecionamento e retorno. Os journals registram `committed` com época 1 e `closed` com época 2 nos dois Masters. A primeira entrada `committed` do journal do destino usa o rótulo local atribuído antes do espelhamento. O UUID é o mesmo e o relatório do destino usa o rótulo da origem. O campo `neighbors` contém o peer configurado em cada Master, com estado `available` e horário do último contato observado.

- [NDJSON original](loopback-20261126-171809/wire.ndjson), capturado antes de encaminhar cada linha ao Collector.
- [Relatórios aceitos](loopback-20261126-171809/reports.jsonl) e [CSV](loopback-20261126-171809/reports.csv), gerados pelo Collector.
- [Ledger da origem](loopback-20261126-171809/grupo_07_master_01.json) e [journal da origem](loopback-20261126-171809/grupo_07_master_01.jsonl).
- [Ledger do destino](loopback-20261126-171809/grupo_07_master_02.json) e [journal do destino](loopback-20261126-171809/grupo_07_master_02.jsonl).
- [Log da execução](captura-verificada.log), incluindo os ACKs de registro e retorno e a correspondência de `loan_id` com `message_id`.

A execução final de `python -m pytest -q tests/test_issue5_telemetry.py` passou com 3 testes. A execução final de `python -m pytest -q` passou com 153 testes. `git diff --check` retornou código 0.
