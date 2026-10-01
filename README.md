# Nome: Mateus Cavalcante Rodrigues/ RA:22303356

# P2P com Balanceamento de Carga Dinâmico

## Objetivo da Sprint 1
A Sprint 1 estabelece a base de comunicação e coordenação do sistema P2P, cobrindo identidade persistente, transporte TCP nativo, enquadramento NDJSON, registro de workers, heartbeat, liveness, reconexão, retransmissão e deduplicação. O objetivo é garantir que o master e os workers se comuniquem de forma confiável, com rastreio de identidade e resposta a falhas de rede sem duplicar registros ou efeitos lógicos.

## Tecnologias
- Python
- sockets TCP nativos
- JSON/NDJSON
- threading
- pytest

## Estrutura principal do projeto
- [Atividade_Pratica_01_ASD.md](Atividade_Pratica_01_ASD.md) — documento da prática e requisitos iniciais da Sprint 1.
- [protocol.py](protocol.py) — validação do protocolo, serialização NDJSON, identidade, liveness e deduplicação.
- [master/master.py](master/master.py) — processo do master, registro de workers, tratamento de clientes e heartbeat mestre.
- [master/config.json](master/config.json) — configuração persistente do master.
- [worker/worker.py](worker/worker.py) — processo do worker, registro, heartbeat e reconexão.
- [worker/config.json](worker/config.json) — configuração persistente do worker.
- [tests/test_protocol.py](tests/test_protocol.py) — suíte de testes da Sprint 1, incluindo aceitação e regressão.
- [demo_tcp_ndjson.py](demo_tcp_ndjson.py) — demo de comunicação TCP/NDJSON.
- [validate_lab_reconnect.py](validate_lab_reconnect.py) — validação de reconexão.

## Identidade dos nós
Cada nó do sistema usa uma identidade persistente definida por:
- group_label
- node_id em formato UUID v4
- node_label
- node_role
- persistência em arquivo de configuração

A identidade é mantida ao longo de reinicializações do processo. O master rejeita conflitos de UUID, label e grupo para evitar registros inconsistentes ou duplicados.

## Protocolo
A comunicação usa TCP nativo com payload em JSON por linha (NDJSON), codificado em UTF-8. Cada mensagem é separada por LF e o processo aceita:
- fragmentação de mensagens em vários writes
- coalescência de múltiplas mensagens em um recv
- limite máximo de 65536 bytes por linha
- rejeição de JSON inválido
- rejeição de chaves duplicadas
- rejeição de tipos/campos inválidos
- rejeição de NaN/Infinity

O envelope mínimo da mensagem segue o padrão oficial da Sprint 1:
- version
- type
- message_id
- request_id
- timestamp_ms
- source
- destination
- payload

Versão atual do protocolo:
- version: sd_2026_2_v1

## Funcionalidades da Sprint 1
A Sprint 1 inclui as seguintes capacidades:
- registro de Worker
- registration_ack
- dois Workers simultâneos
- heartbeat bidirecional
- assignment_epoch
- transição de liveness: available -> suspect -> unavailable
- detecção após 3 perdas consecutivas
- reconexão automática
- backoff crescente até 5 segundos
- timeout de 5 segundos
- máximo de 3 envios
- deduplicação por (source.node_id, request_id, type)
- protocol_error

## Execução no Windows PowerShell
Os processos devem ser iniciados em terminais separados.

### Master
```powershell
python .\master\master.py --host 127.0.0.1 --port 9000
```

### Worker A
```powershell
python .\worker\worker.py --config .\worker\worker-a.json
```

### Worker B
```powershell
python .\worker\worker.py --config .\worker\worker-b.json
```

## Como executar os testes
```powershell
python -m pytest -q
```

Resultado final validado da suíte atual:
- 59 passed in 8.74s

## Cenário manual validado
O cenário abaixo foi validado manualmente no ambiente local:
1. iniciar o Master
2. iniciar o Worker A
3. iniciar o Worker B
4. verificar registro e heartbeat dos dois workers
5. interromper o Worker A
6. verificar o Worker A como indisponível
7. verificar que o Worker B continua funcionando normalmente
8. iniciar o Worker A novamente
9. verificar novo registro e reconexão com heartbeat restaurado

## Testes de aceitação
A suíte inclui os cenários de aceitação:
- CT_01 — framing NDJSON
- CT_02 — múltiplos workers, reconexão e conflito de identidade
- CT_03 — mensagens inválidas, protocol_error e continuidade do processo

## Logs
O sistema registra eventos em um único formato padronizado:
- grupo | origem | destino | type | request_id | resultado

Os exemplos abaixo são apenas ilustrativos do formato e não representam IDs reais nem evidências de execução:
- ASD | worker-a | master | registration_ack | <request_id> | accepted
- ASD | master | worker-a | heartbeat_ack | <request_id> | sent
- ASD | worker-a | master | heartbeat | socket error | unavailable

## Evidências da Sprint 1
Os seguintes pontos foram validados na branch sprint-1:
- Master ativo
- dois Workers simultâneos
- heartbeat bidirecional
- queda isolada do Worker A
- continuidade do Worker B
- retorno do Worker A com reconexão
- 59 testes automatizados em verde

## Escopo da Sprint 1
Esta documentação se limita à Sprint 1. Não inclui funcionalidades futuras da Sprint 2, que não fazem parte do escopo atual desta branch.
