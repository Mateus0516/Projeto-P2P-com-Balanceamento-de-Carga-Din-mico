# Atividade Prática 01: Enquadramento NDJSON e Handshake de Registro

## Contexto e Objetivo
Esta é a atividade inicial do projeto P2P com Balanceamento de Carga Dinâmico e serve de preparação direta para a Sprint 1 [cite: 1]. O objetivo é que cada grupo saia do laboratório com um canal TCP confiável entre dois processos distintos, falando um protocolo de mensagens em JSON por linha, com identidade persistente [cite: 1]. É o alicerce exigido pela Sprint 1, construído em escala reduzida para que os erros apareçam agora, e não na véspera [cite: 1].

---

## Roteiro

1. **Separação e Configuração:**
   Partindo do material *Exemplo Socket* disponível na sala, separar o código em dois executáveis: `master.py` e `worker.py` [cite: 1]. Cada um deve possuir seu próprio arquivo de configuração (`config.json`), contendo um UUID gerado na primeira execução e persistido, além de `label`, `host` e `port` [cite: 1].

2. **Enquadramento NDJSON:**
   Implementar `send_msg` e `recv_msgs` utilizando enquadramento NDJSON: cada mensagem é um JSON serializado em uma linha, terminada por quebra de linha (`
`) [cite: 1]. O receptor deve manter um buffer que acumula bytes parciais e só entrega linhas completas [cite: 1].

3. **Envelope Mínimo e Validação:**
   Definir e validar o envelope mínimo da mensagem com os seguintes campos:
   - `type` [cite: 1]
   - `msg_id` [cite: 1]
   - `request_id` [cite: 1]
   - `origin` (UUID e label) [cite: 1]
   - `timestamp` [cite: 1]
   - `payload` [cite: 1]
   
   *Regra:* Mensagens sem campos obrigatórios ou com JSON inválido devem ser descartadas com registro em log, nunca derrubando o processo [cite: 1].

4. **Handshake e Tabela de Workers:**
   Implementar o par `register_worker` e `registration_ack` [cite: 1]. O *master* deve manter uma tabela de workers em memória indexada por UUID [cite: 1]. Reconexões do mesmo UUID devem apenas atualizar o registro existente na tabela, em vez de criar duplicatas [cite: 1].

5. **Padronização de Logs:**
   Registrar logs em uma única linha por evento, seguindo estritamente o formato:  
   `grupo | origem | destino | type | request_id | resultado` [cite: 1]

---

## Testes a Executar no Laboratório

Cada grupo deve demonstrar os cinco cenários abaixo em execução, com os logs à vista:

* **Fragmentação:** Enviar um JSON grande em dois `send` e confirmar que chega inteiro no receptor [cite: 1].
* **Coalescência:** Enviar três mensagens em rajada e confirmar que o receptor entrega exatamente três mensagens distintas, e não uma única concatenada [cite: 1].
* **JSON Inválido:** Enviar conteúdo malformado e confirmar que o processo sobrevive sem interromper a execução [cite: 1].
* **Identidade Duplicada:** Subir dois workers utilizando o mesmo UUID e descrever o comportamento escolhido [cite: 1].
* **Reconexão:** Encerrar um worker, subi-lo novamente e demonstrar que a tabela do *master* permanece com um único registro atualizado [cite: 1].

---

## Resultado Esperado

Um *master* e pelo menos um *worker* executando como processos independentes, identificáveis pelo `label` nos logs, trocando mensagens enquadradas corretamente mesmo sob fragmentação e rajada, com registro reconhecido pelo *master* e reconexão sem duplicar o cadastro [cite: 1].
