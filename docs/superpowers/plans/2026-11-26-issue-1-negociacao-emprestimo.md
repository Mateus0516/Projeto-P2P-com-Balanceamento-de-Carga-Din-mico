# Issue 1 — Negociação e Empréstimo da Sprint 3

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans for implementation tasks.

**Goal:** Corrigir integralmente negociação, reserva, commit, redirect, retorno e reconciliação da Sprint 3 conforme o plano oficial.

**Architecture:** Centralizar loan_ref e validadores em protocol.py; tornar PeerLoanManager autoritativo para ledger, journal JSONL, reserva atômica e epoch; preservar WorkerSessionManager para redirect, retorno e reconciliação.

**Tech Stack:** Python 3.14, sockets TCP, NDJSON, threading, pytest, JSON/JSONL.

**Spec:** docs/referencias-professor/plano_proj_SD-26_2 (2).pdf, seções 3, 4.2, 4.3, 5.1–5.5, 6.3 e 7.

## Global Constraints

- Preserve sd_2026_2_v1.
- workers_needed = 1.
- min_home_workers = 1.
- negotiation_cooldown_ms = 5000.
- reservation_timeout_ms = 15000.
- Origem oferece somente com fila <= release_threshold.
- Saturação > saturation_threshold por três amostras; liberação <= release_threshold por três amostras.
- Cada operação usa request_id próprio; respostas e retransmissões preservam IDs e conteúdo.
- assignment_epoch começa em 0, incrementa no commit e no retorno confirmado, sem repetir incrementos.
- Persistir decisões autoritativas em journal JSONL.
- Não inventar mensagens, campos ou ACKs além do catálogo 5.3.
- Não alterar arquivos oficiais de docs/referencias-professor.
- Não alterar collector.py de produção.

---

### Task 1 — Payloads oficiais e loan_ref

**Files:**
- Modify: protocol.py
- Modify: tests/test_protocol.py
- Modify: tests/test_sprint3_dynamic_load.py

- [ ] Adicionar testes de response_accepted, response_rejected, confirm_loan, loan_committed, command_redirect, register_temporary_worker, notify/return_ack, loan_status e cancel.
- [ ] Verificar falha dos payloads atuais.
- [ ] Implementar loan_ref completo e validadores oficiais.
- [ ] Atualizar builders sem manter campos extras nos payloads oficiais.
- [ ] Executar testes focados até verde.

### Task 2 — Reserva concorrente e cooldown

**Files:**
- Modify: master/master.py
- Modify: tests/test_protocol.py
- Modify: tests/test_sprint3_dynamic_load.py

- [ ] Adicionar teste de duas requisições concorrentes sobre um worker.
- [ ] Adicionar teste de offered/committed bloqueando nova oferta e task_request.
- [ ] Adicionar teste de oferta somente com fila local <= release_threshold.
- [ ] Implementar reserva, capacidade e reserva mínima sob o mesmo lock.
- [ ] Implementar cooldown de 5000 ms por negociação.
- [ ] Executar testes focados até verde.

### Task 3 — Journal, label e epoch

**Files:**
- Modify: master/master.py
- Modify: tests/test_protocol.py
- Modify: tests/test_sprint3_dynamic_load.py

- [ ] Adicionar testes para sequência persistente lower_snake_case.
- [ ] Adicionar testes para journal offered/committed/closed.
- [ ] Adicionar testes para commit idempotente e retorno idempotente.
- [ ] Persistir label e loan_sequence no ledger/configuração apropriada.
- [ ] Incrementar epoch no commit e no retorno sem dupla incrementação.
- [ ] Executar testes focados até verde.

### Task 4 — Heartbeat e reconciliação

**Files:**
- Modify: protocol.py
- Modify: master/master.py
- Modify: worker/worker.py
- Modify: tests/test_protocol.py
- Modify: tests/test_sprint3_dynamic_load.py

- [ ] Adicionar heartbeat durante offered e committed.
- [ ] Adicionar reconexão com loan_id/null e epoch anterior.
- [ ] Adicionar perda de confirm, redirect e notify/return_ack.
- [ ] Preservar estados offered/committed em heartbeat e reconexão.
- [ ] Implementar get_loan_status e reenvio reconciliado somente pelos mecanismos previstos.
- [ ] Executar testes focados até verde.

### Task 5 — CT_07 a CT_13 e integração

**Files:**
- Modify: tests/test_protocol.py
- Create: tests/test_issue1_acceptance.py
- Create: evidencias/issue-1 scripts/logs/resultados conforme necessário

- [ ] Executar CT_07, CT_08, CT_09 com disputa real por três masters em teste integrado.
- [ ] Executar CT_10, CT_11, CT_12 e CT_13 com falhas controladas.
- [ ] Executar dois masters e dois workers por master com 200 tarefas sum_range 1..1000, chegada 20 ms e delay 1000 ms.
- [ ] Registrar logs, mensagens, ledger/journal e resultado individual.
- [ ] Executar suíte completa.
- [ ] Executar git diff --check e git status --short.
