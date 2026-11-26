# Resultados dos ensaios TCP locais da Sprint 4

Os três ensaios executaram processos separados em loopback com os dois Masters e os quatro workers do grupo 07, além do Collector de referência. O código-fonte executado e a carga são idênticos nas três pastas, conforme os hashes registrados em `parametros.json` e a comparação de `carga.json`. Os resultados demonstram os dois papéis com a implementação local. A aprovação do CT_14 requer execução com outra equipe independente.

## Comparação com a carga oficial

O perfil usou 200 tarefas `sum_range(1, 1000)`, seed 44, chegadas previstas a cada 20 ms e demora de execução de 1.000 ms. O tempo começa na primeira submissão e termina quando o roteiro observa a conclusão das 200 tarefas. O Master publica esse snapshot a cada segundo. A sonda de indisponibilidade começa após a medição.

| Cenário | Master sob carga | Tempo (s) | Pico de pendências por snapshot | Concluídas | Resultados aceitos do worker cedido | Amostras aceitas / rejeitadas |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| [Sem ajuda](local-sem-ajuda-a-20261126-173512-310933/resultado.json) | `grupo_07_master_01` | 102,143 | 192 | 200 | 0 | 24 / 0 |
| [Com ajuda](local-com-ajuda-a-20261126-173511-974216/resultado.json) | `grupo_07_master_01` | 69,772 | 191 | 200 | 64 | 18 / 0 |
| [Com ajuda, papéis invertidos](local-com-ajuda-b-20261126-173512-409963/resultado.json) | `grupo_07_master_02` | 69,870 | 191 | 200 | 64 | 18 / 0 |

No mesmo Master 01, a diferença observada foi de 32,371 s. Compare também a fila e o avanço no mesmo instante de amostragem. O tempo medido depende da execução.

| Cenário e instante de uptime | Master | Fila pendente | Concluídas | Workers ativos | Ociosos | Cedidos | Recebidos |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Sem ajuda, antes (10 s) | `grupo_07_master_01` | 0 | 0 | 2 | 2 | 0 | 0 |
| Sem ajuda, durante (50 s) | `grupo_07_master_01` | 120 | 78 | 2 | 0 | 0 | 0 |
| Sem ajuda, depois (120 s) | `grupo_07_master_01` | 0 | 200 | 2 | 2 | 0 | 0 |
| Com ajuda, antes (10 s) | `grupo_07_master_01` | 0 | 0 | 2 | 2 | 0 | 0 |
| Com ajuda, durante (50 s) | `grupo_07_master_01` | 86 | 111 | 3 | 0 | 0 | 1 |
| Com ajuda, depois (90 s) | `grupo_07_master_01` | 0 | 200 | 2 | 2 | 0 | 0 |
| Com ajuda, cedente durante (50 s) | `grupo_07_master_02` | 0 | 0 | 1 | 1 | 1 | 0 |
| Com ajuda, cedente após retorno (90 s) | `grupo_07_master_02` | 0 | 0 | 2 | 2 | 0 | 0 |
| Com ajuda, papéis invertidos, antes (10 s) | `grupo_07_master_02` | 0 | 0 | 2 | 2 | 0 | 0 |
| Com ajuda, papéis invertidos, durante (50 s) | `grupo_07_master_02` | 85 | 112 | 3 | 0 | 0 | 1 |
| Com ajuda, papéis invertidos, depois (90 s) | `grupo_07_master_02` | 0 | 200 | 2 | 2 | 0 | 0 |
| Com ajuda, papéis invertidos, cedente durante (50 s) | `grupo_07_master_01` | 0 | 0 | 1 | 1 | 1 | 0 |
| Com ajuda, papéis invertidos, cedente após retorno (90 s) | `grupo_07_master_01` | 0 | 0 | 2 | 2 | 0 | 0 |

## Negociação, devolução e indisponibilidade

- `grupo_07_master_02` cedeu `grupo_07_master_02_worker_02` a `grupo_07_master_01`. Empréstimo `243cb31b-f97d-45de-9754-01a6d952cf85`. Registro em ambos os ledgers como `closed`, com época 2, e `return_ack` correlacionado por `e78a7ec9-c664-46e2-b523-8fbcb4d6ffd2`. Exemplo de ACK de resultado aceito pelo receptor `01262d99-22a5-4e60-92de-47aab8c88027`. [Consulta inicial nos dois sentidos](local-com-ajuda-a-20261126-173511-974216/preflight.json), [ledgers e journals](local-com-ajuda-a-20261126-173511-974216/ledgers) e [relatórios do Collector](local-com-ajuda-a-20261126-173511-974216/collector/reports.jsonl).
- `grupo_07_master_01` cedeu `grupo_07_master_01_worker_02` a `grupo_07_master_02`. Empréstimo `fd967e6f-ac66-4f73-b9ec-2202acf10fb5`. Registro em ambos os ledgers como `closed`, com época 2, e `return_ack` correlacionado por `87784d31-0baf-43a6-921c-b5ca618599a4`. Exemplo de ACK de resultado aceito pelo receptor `01e6a1f5-5702-4f6c-8cb8-97a846d4874c`. [Consulta inicial nos dois sentidos](local-com-ajuda-b-20261126-173512-409963/preflight.json), [ledgers e journals](local-com-ajuda-b-20261126-173512-409963/ledgers) e [relatórios do Collector](local-com-ajuda-b-20261126-173512-409963/collector/reports.jsonl).
- A sonda na fila saturada recebeu `response_rejected` com `no_workers_available`. [Pedido e resposta originais](local-com-ajuda-a-20261126-173511-974216/sonda-recusa.json).
- Depois do encerramento do Master 02 local, o Master 01 detectou a falha de conexão, continuou com dois workers e concluiu as 30 tarefas suplementares, totalizando 230 conclusões e 0 falhas. O Collector aceitou a amostra `17de5e4c-a7d0-4bff-ace8-68a77fd5e975`, com o vizinho em `unavailable`. [Log do Master 01](local-com-ajuda-a-20261126-173511-974216/master-a.log) e [amostras da sonda](local-com-ajuda-a-20261126-173511-974216/collector/reports.jsonl).

As pastas de tentativas anteriores foram preservadas. Uma execução inicial foi bloqueada pelo diretório temporário do sandbox. Uma sonda anterior encerrou ao exigir um motivo de recusa específico, e outra usou uma carga adicional que drenou antes de satisfazer a histerese. A tabela registra o trio completo anterior à revisão de tratamento de falhas.
