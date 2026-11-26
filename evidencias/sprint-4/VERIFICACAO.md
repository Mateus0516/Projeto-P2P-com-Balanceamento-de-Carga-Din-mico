# Verificação das Sprints 1 a 4

Base `d1acba7`, branch local `sprint-4`. Referência do professor nas seções 3 a 8 do plano e contrato `sd_2026_2_v1`. Os ensaios usaram TCP local com processos separados, sem contato com o Collector central.

| Caso | Verificação local |
| --- | --- |
| CT_01 | Testes TCP de mensagens fragmentadas, agrupadas e com limite excedido. |
| CT_02 | Testes de cadastro, conflito de identidade/label, perda e reconexão do worker e reaplicação de ACK. |
| CT_03 | Testes de mensagens inválidas, tipos desconhecidos e recuperação da conexão. |
| CT_04 | Testes de execução concorrente, soma correta e `no_task`. Ensaios de 200 tarefas com dois workers por origem. |
| CT_05 | Testes de retransmissão de `task_result`, ACK duplicado, contabilização única e reconciliação. |
| CT_06 | Testes de reatribuição após perda de worker e rejeição de tentativa obsoleta. |
| CT_07 | Testes das três amostras de saturação e liberação. Os relatórios do Collector local mostram a evolução da fila. |
| CT_08 | Empréstimo, execução e retorno dos workers próprios em ambos os sentidos no ensaio local. |
| CT_09 | Teste concorrente com dois solicitantes e um único worker disponível, mantendo uma única reserva. |
| CT_10 | Oferta expirada em teste TCP. O Master devolveu `reservation_expired`, manteve o worker disponível e não confirmou o empréstimo. |
| CT_11 | Perda da resposta depois do commit em teste TCP. O solicitante consultou `get_loan_status` e espelhou a decisão autoritativa. Um teste separado exercitou a falha no próprio envio de `loan_committed` e confirmou que o worker recebeu o redirect. |
| CT_12 | Registro temporário anterior ao commit recusado. O mesmo pedido de confirmação devolveu a mesma decisão. Testes do worker rejeitam época obsoleta e origem incorreta. |
| CT_13 | Teste TCP fecha a conexão do receptor e verifica o registro de retorno na origem. O teste dos Masters repete o registro e a notificação e confere a mesma época final nos dois lados. |
| CT_14 | Roteiro, configuração, preflight e ensaio preparatório disponíveis. A aceitação depende de usar a implementação de outra equipe em ambos os sentidos. |

Os testes de regressão ficam em `tests/test_protocol.py`, `tests/test_sprint3_dynamic_load.py` e `tests/test_issue5_telemetry.py`. Os cenários de contrato e falha adicionados ficam em `tests/test_sprint4_contract.py` e `tests/test_sprint4_worker.py`. A suíte dirigida para esses quatro arquivos de Sprint 3/4 e telemetria passou com **20 testes**. A suíte completa `python -m pytest -q` passou com **164 testes**, sem falhas. `git diff --check` retornou código 0.

O trabalho corrigiu o formato de transferência e retorno, a consulta de estado, o vínculo de identidade e época, a recuperação após perda de confirmação e o tratamento de regressos no journal. O preflight consulta o Master configurado pela operação oficial `get_loan_status`. Os perfis das medições usam a mesma semente e parâmetros, com a lista de peers vazia no baseline.

- [Configuração e comandos](README.md)
- [Roteiro e ficha da integração](CT_14.md)
- [Comparação dos ensaios locais](relatorio-ensaios-locais.md)
