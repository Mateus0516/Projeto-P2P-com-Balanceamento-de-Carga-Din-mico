# Resultados dos ensaios TCP locais da Sprint 4

Os três ensaios executaram processos separados em loopback com os dois Masters e os quatro workers do grupo 07, além do Collector de referência. O código-fonte executado e a carga são idênticos nas três pastas, conforme os hashes registrados em `parametros.json` e a comparação de `carga.json`. Os resultados demonstram os dois papéis com a implementação local. A aprovação do CT_14 requer execução com outra equipe independente.

## Comparação com a carga oficial

O perfil usou 200 tarefas `sum_range(1, 1000)`, seed 44, chegadas previstas a cada 20 ms e demora de execução de 1.000 ms. O tempo começa na primeira submissão e termina quando o roteiro observa a conclusão das 200 tarefas. O Master publica esse snapshot a cada segundo. A sonda de indisponibilidade começa após a medição.

| Cenário | Master sob carga | Tempo (s) | Pico de pendências por snapshot | Concluídas | Resultados aceitos do worker cedido | Amostras aceitas / rejeitadas |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| [Sem ajuda](local-sem-ajuda-a-20261126-173713-961845/resultado.json) | `grupo_07_master_01` | 101,971 | 192 | 200 | 0 | 24 / 0 |
| [Com ajuda](local-com-ajuda-a-20261126-173713-935666/resultado.json) | `grupo_07_master_01` | 69,692 | 191 | 200 | 64 | 18 / 0 |
| [Com ajuda, papéis invertidos](local-com-ajuda-b-20261126-173713-772787/resultado.json) | `grupo_07_master_02` | 69,681 | 191 | 200 | 65 | 18 / 0 |

No mesmo Master 01, a diferença observada foi de 32,279 s. Compare também a fila e o avanço no mesmo instante de amostragem. O tempo medido depende da execução.

| Cenário e instante de uptime | Master | Fila pendente | Concluídas | Workers ativos | Ociosos | Cedidos | Recebidos |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Sem ajuda, antes (10 s) | `grupo_07_master_01` | 0 | 0 | 2 | 2 | 0 | 0 |
| Sem ajuda, durante (50 s) | `grupo_07_master_01` | 122 | 76 | 2 | 0 | 0 | 0 |
| Sem ajuda, depois (120 s) | `grupo_07_master_01` | 0 | 200 | 2 | 2 | 0 | 0 |
| Com ajuda, antes (10 s) | `grupo_07_master_01` | 0 | 0 | 2 | 2 | 0 | 0 |
| Com ajuda, durante (50 s) | `grupo_07_master_01` | 86 | 111 | 3 | 0 | 0 | 1 |
| Com ajuda, depois (90 s) | `grupo_07_master_01` | 0 | 200 | 2 | 2 | 0 | 0 |
| Com ajuda, cedente durante (50 s) | `grupo_07_master_02` | 0 | 0 | 1 | 1 | 1 | 0 |
| Com ajuda, cedente após retorno (90 s) | `grupo_07_master_02` | 0 | 0 | 2 | 2 | 0 | 0 |
| Com ajuda, papéis invertidos, antes (10 s) | `grupo_07_master_02` | 0 | 0 | 2 | 2 | 0 | 0 |
| Com ajuda, papéis invertidos, durante (50 s) | `grupo_07_master_02` | 83 | 114 | 3 | 0 | 0 | 1 |
| Com ajuda, papéis invertidos, depois (90 s) | `grupo_07_master_02` | 0 | 200 | 2 | 2 | 0 | 0 |
| Com ajuda, papéis invertidos, cedente durante (50 s) | `grupo_07_master_01` | 0 | 0 | 1 | 1 | 1 | 0 |
| Com ajuda, papéis invertidos, cedente após retorno (90 s) | `grupo_07_master_01` | 0 | 0 | 2 | 2 | 0 | 0 |

## Negociação, devolução e indisponibilidade

- `grupo_07_master_02` cedeu `grupo_07_master_02_worker_01` a `grupo_07_master_01`. Empréstimo `d66890b9-ff33-47f0-a978-974e81a0ec81`. Registro em ambos os ledgers como `closed`, com época 2, e `return_ack` correlacionado por `5c2ba341-b091-4db6-a010-375a854417ef`. Exemplo de ACK de resultado aceito pelo receptor `011904ff-d4ee-46b5-8532-444da1d1d97e`. [Consulta inicial nos dois sentidos](local-com-ajuda-a-20261126-173713-935666/preflight.json), [ledgers e journals](local-com-ajuda-a-20261126-173713-935666/ledgers) e [relatórios do Collector](local-com-ajuda-a-20261126-173713-935666/collector/reports.jsonl).
- `grupo_07_master_01` cedeu `grupo_07_master_01_worker_02` a `grupo_07_master_02`. Empréstimo `3c61fe94-5792-498a-b2c8-8b2b84b8444b`. Registro em ambos os ledgers como `closed`, com época 2, e `return_ack` correlacionado por `f66a283b-a500-489e-85f8-2c56b496357e`. Exemplo de ACK de resultado aceito pelo receptor `05cc0d0c-711a-4d7b-af61-b5afaf0e8481`. [Consulta inicial nos dois sentidos](local-com-ajuda-b-20261126-173713-772787/preflight.json), [ledgers e journals](local-com-ajuda-b-20261126-173713-772787/ledgers) e [relatórios do Collector](local-com-ajuda-b-20261126-173713-772787/collector/reports.jsonl).
- A sonda na fila saturada recebeu `response_rejected` com `no_workers_available`. [Pedido e resposta originais](local-com-ajuda-a-20261126-173713-935666/sonda-recusa.json).
- Depois do encerramento do Master 02 local, o Master 01 detectou a falha de conexão, continuou com dois workers e concluiu as 30 tarefas suplementares, totalizando 230 conclusões e 0 falhas. O Collector aceitou a amostra `8fdbfd6f-55b6-4f48-ac06-3d46b60e988f`, com o vizinho em `unavailable`. [Log do Master 01](local-com-ajuda-a-20261126-173713-935666/master-a.log) e [amostras da sonda](local-com-ajuda-a-20261126-173713-935666/collector/reports.jsonl).

As pastas de tentativas anteriores foram preservadas. Uma execução inicial foi bloqueada pelo diretório temporário do sandbox. Uma sonda anterior encerrou ao exigir um motivo de recusa específico, e outra usou uma carga adicional que drenou antes de satisfazer a histerese. A tabela usa as três execuções da versão revisada. [Comparação anterior à revisão](relatorio-ensaios-locais-previos.md).
