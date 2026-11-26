# Sprint 4 do grupo 07

O plano do professor define a Sprint 4 na seção 6.4, os testes de aceitação na seção 7 e a telemetria na seção 8. O contrato de rede é `sd_2026_2_v1`, com mensagens NDJSON em sockets TCP. Masters e workers executam em processos separados.

- [Manifesto do grupo 07](manifesto-grupo-07.json)
- [Modelo de manifesto do parceiro](manifesto-parceiro.modelo.json)
- [Roteiro do CT_14](CT_14.md)

O manifesto do grupo 07 traz as identidades persistentes dos dois Masters e quatro workers disponíveis para ensaios locais. Os endereços atuais dos Masters são de loopback. Durante a troca de manifestos, cada grupo informa o IP alcançável e a porta reais do Master participante, preservando seus UUIDs e labels. O modelo do parceiro deixa os dados que dependem da outra equipe em `null`. O Collector indicado é o destino externo oficial da telemetria.

## Configuração dos peers

1. Troque os manifestos com a equipe independente. Compare `version`, confira se os grupos são distintos e verifique se cada Master tem UUIDv4 persistente e label da forma `grupo_XX_master_NN`. Cada worker mantém o UUID, o grupo, o label e o `home_master` de origem.
2. Crie cópias das configurações do Master e dos dois workers escolhidos para cada execução. Atualize o `host` e a `port` do Master para o endereço local que o parceiro pode alcançar, e atualize a referência `master` dos dois workers com a mesma identidade e endereço. Confirme as regras de firewall do ambiente para a porta TCP do Master. O worker abre a conexão e dispensa porta de escuta.
3. Em `peers`, copie exatamente do manifesto confirmado do parceiro `group_label`, `node_id`, `node_label`, `node_role`, `host` e `port`. O parceiro configura o nosso Master da mesma maneira. No perfil sem ajuda use `"peers": []`. Para o par local, aproveite as referências recíprocas presentes em `master-a.json` e `master-b.json`.
4. Configure `loan_ledger_path` com um caminho novo por Master e execução. O journal JSONL usa o mesmo nome-base. Crie o diretório de resultados antes de iniciar. As cópias dos workers começam com `assignment_epoch: 0` e `loan_id: null` em uma execução isolada que usa ledgers vazios. Na retomada de um ensaio interrompido, preserve os ledgers e as identidades daquela execução.
5. Para desenvolvimento, configure `collector_host: "127.0.0.1"` e `collector_port: 9100` nas cópias locais. Rode o Collector de referência em cada máquina em que precisar observar telemetria localmente. Preserve a identidade oficial do manifesto e `performance_interval_seconds: 10`. Use o endereço central apenas quando o professor confirmar a disponibilidade do serviço.
6. Depois de ambos os Masters iniciarem, valide conectividade e identidade nos dois sentidos com `python -m client.peer_check --config CAMINHO_DA_COPIA` (Windows) ou `python3 -m client.peer_check --config CAMINHO_DA_COPIA` (macOS). A consulta usa o catálogo oficial e confirma a versão e a identidade da resposta. A disponibilidade indica que o peer respondeu ao protocolo. A concessão depende de um pedido de ajuda e da política de carga.

## Perfil de execução

| Parâmetro | Valor |
| --- | --- |
| Workers por Master | 2 |
| Tarefas | 200 de `sum_range` |
| Intervalo da soma | 1 a 1000 |
| Intervalo entre chegadas | 20 ms |
| `execution_delay_ms` | 1000 |
| `saturation_threshold` | 10 |
| `release_threshold` | 3 |
| `stable_samples` | 3 |
| `monitor_interval_ms` | 1000 |
| `min_home_workers` | 1 |
| `negotiation_cooldown_ms` | 5000 |
| `reservation_timeout_ms` | 15000 |
| `response_timeout_ms` | 5000 |
| Telemetria | 10 s |
| Semente escolhida para os pares de ensaios | 44 |

## Execução no Windows

Na raiz do repositório, prepare pastas e faça cópias das configurações. Use o mesmo valor de `$run` nos terminais da execução, substituindo pelo caminho impresso na preparação.

```powershell
$run = "evidencias/sprint-4/execucao-$(Get-Date -Format yyyyMMdd-HHmmss)"
New-Item -ItemType Directory -Path $run
Copy-Item master/master-a.json,master/master-b.json,worker/worker-a.json,worker/worker-a2.json,worker/worker-b.json,worker/worker-b2.json $run
Write-Output $run
```

Edite as cópias conforme a seção de configuração. Use um terminal por comando. Para o ensaio local com os dois Masters próprios, abra também as três linhas referentes a `master-b`, `worker-b` e `worker-b2`.

```powershell
python -u docs/referencias-professor/collector.py --host 127.0.0.1 --port 9100 --data-dir "$run/collector" 2>&1 | Tee-Object "$run/collector.log"
python -u -m master.master --config "$run/master-a.json" --snapshot-interval-seconds 1 2>&1 | Tee-Object "$run/master-a.log"
python -u -m worker.worker --config "$run/worker-a.json" 2>&1 | Tee-Object "$run/worker-a.log"
python -u -m worker.worker --config "$run/worker-a2.json" 2>&1 | Tee-Object "$run/worker-a2.log"
python -u -m master.master --config "$run/master-b.json" --snapshot-interval-seconds 1 2>&1 | Tee-Object "$run/master-b.log"
python -u -m worker.worker --config "$run/worker-b.json" 2>&1 | Tee-Object "$run/worker-b.log"
python -u -m worker.worker --config "$run/worker-b2.json" 2>&1 | Tee-Object "$run/worker-b2.log"
```

Após dois registros aceitos por Master e uma amostra inicial do Collector, submeta a carga ao Master sob teste. Para inverter o papel no par local, repita a preparação com uma pasta nova e use a porta 9001.

```powershell
python -u -m client.load_generator --host 127.0.0.1 --port 9000 --task-count 200 --start 1 --end 1000 --interval-ms 20 --seed 44 2>&1 | Tee-Object "$run/carga.log"
```

## Execução no macOS

Na raiz do repositório, prepare as cópias. Use o mesmo valor de `run` nos demais terminais.

```sh
run="evidencias/sprint-4/execucao-$(date +%Y%m%d-%H%M%S)"
mkdir "$run"
cp master/master-{a,b}.json worker/worker-{a,a2,b,b2}.json "$run"
printf '%s\n' "$run"
```

Edite as cópias conforme a seção de configuração. Abra um terminal para cada comando aplicável.

```sh
python3 -u docs/referencias-professor/collector.py --host 127.0.0.1 --port 9100 --data-dir "$run/collector" 2>&1 | tee "$run/collector.log"
python3 -u -m master.master --config "$run/master-a.json" --snapshot-interval-seconds 1 2>&1 | tee "$run/master-a.log"
python3 -u -m worker.worker --config "$run/worker-a.json" 2>&1 | tee "$run/worker-a.log"
python3 -u -m worker.worker --config "$run/worker-a2.json" 2>&1 | tee "$run/worker-a2.log"
python3 -u -m master.master --config "$run/master-b.json" --snapshot-interval-seconds 1 2>&1 | tee "$run/master-b.log"
python3 -u -m worker.worker --config "$run/worker-b.json" 2>&1 | tee "$run/worker-b.log"
python3 -u -m worker.worker --config "$run/worker-b2.json" 2>&1 | tee "$run/worker-b2.log"
```

Dispare a carga depois dos registros e da primeira amostra. Use o endereço e a porta anunciados pelo Master em ensaios entre máquinas.

```sh
python3 -u -m client.load_generator --host 127.0.0.1 --port 9000 --task-count 200 --start 1 --end 1000 --interval-ms 20 --seed 44 2>&1 | tee "$run/carga.log"
```


## Ensaio local automatizado

O [roteiro automatizado](ensaio_local.py) usa os dois Masters próprios, os quatro workers em processos separados e o Collector de referência em loopback. Cada execução tem portas, configurações, logs e ledgers isolados. Use `python3` no macOS.

```text
python evidencias/sprint-4/ensaio_local.py sem-ajuda --ativo a --seed 44
python evidencias/sprint-4/ensaio_local.py com-ajuda --ativo a --seed 44 --sondas
python evidencias/sprint-4/ensaio_local.py sem-ajuda --ativo b --seed 44
python evidencias/sprint-4/ensaio_local.py com-ajuda --ativo b --seed 44
```

O modo `--sondas` acrescenta as verificações locais de recusa e indisponibilidade. A ficha do CT_14 requer a repetição dos cenários com a equipe independente.
