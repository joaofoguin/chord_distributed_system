# Chord em Node.js — Anel Chord com Replicação de Arquivos

Implementação didática do protocolo **Chord** (DHT — tabela hash distribuída)
com espaço fixo de identificadores `1..32` (`m = 5`) e **replicação de
arquivos com fator 5**. Cada nó mantém cinco entradas na finger table,
conversa com os demais por HTTP (RPC simples em JSON) e oferece uma interface
web para acompanhar o anel, inserir e recuperar arquivos. Requer Node.js 18+
e não usa pacotes externos (só a biblioteca padrão do Node).

## Sobre o projeto

Este repositório é a implementação prática do trabalho de Sistemas
Distribuídos: construir uma rede par-a-par (P2P) usando o protocolo Chord,
com uma proposta de replicação de arquivos por cima do protocolo básico. A
proposta central é simples de descrever e não tão simples de fazer
funcionar direito: alocar um espaço fixo de posições num anel lógico,
mapear cada nó e cada arquivo para uma posição desse anel por hash, e
garantir que qualquer nó da rede consiga localizar (e devolver) qualquer
arquivo — mesmo sem saber de antemão quem o guarda — usando só um punhado
de "atalhos" (a finger table) em vez de perguntar a todo mundo.

Em cima do Chord "de livro", o trabalho adiciona duas coisas que o
protocolo básico não resolve sozinho:

- **Replicação** — cada arquivo fica guardado em 5 nós (o dono + os 4
  sucessores seguintes no anel), não só num único ponto, para sobreviver à
  saída de qualquer um deles sem perder dados.
- **Tolerância a falhas** — a rede se reorganiza sozinha quando um nó cai
  sem avisar (computador desligado, processo encerrado), não só quando ele
  sai educadamente pelo protocolo.

Arquiteturalmente, o sistema é um conjunto de nós HTTP independentes que
conversam entre si por RPC simples (JSON sobre HTTP) para formar e manter o
anel; um painel controlador que sobe e derruba vários desses nós numa mesma
máquina, para facilitar testes e demonstrações; e uma interface web por nó,
para acompanhar o estado do anel e fazer upload/download de arquivos. Não
há coordenador central, banco de dados nem dependência externa — cada nó só
sabe o que aprende conversando com seus vizinhos.

## Sumário

- [Sobre o projeto](#sobre-o-projeto)
- [Visão geral da arquitetura](#visão-geral-da-arquitetura)
- [Como o anel funciona (`ring.js`)](#como-o-anel-funciona-ringjs)
- [O nó Chord (`chord-node.js`)](#o-nó-chord-chord-nodejs)
- [Estratégia de replicação (`replication.js`)](#estratégia-de-replicação-replicationjs)
- [Tolerância a falhas e recuperação do anel](#tolerância-a-falhas-e-recuperação-do-anel)
- [Camada HTTP / RPC (`node-server.js` e `server.js`)](#camada-http--rpc-node-serverjs-e-serverjs)
- [Executar o projeto](#executar-o-projeto)
- [Arquivos: `put` e `get` via API](#arquivos-put-e-get-via-api)
- [Testes automatizados](#testes-automatizados)
- [Guia da apresentação (26/08/2026)](#guia-da-apresentação-26082026)

## Visão geral da arquitetura

```
┌────────────────────────────────────────────────────────────┐
│  src/server.js  → "painel controlador" (porta 5000)         │
│  Sobe/derruba nós ChordNode dentro do mesmo processo Node   │
│  (útil para testar vários nós numa única máquina)            │
└───────────────┬───────────────────────────────────────────┘
                │ cria/gerencia
                ▼
┌────────────────────────────────────────────────────────────┐
│  src/node-server.js → servidor HTTP de CADA nó                │
│  Expõe API pública (upload/download) e rotas /rpc/* usadas    │
│  pelos próprios nós para conversar entre si                   │
└───────────────┬───────────────────────────────────────────┘
                │ usa
                ▼
┌────────────────────────────────────────────────────────────┐
│  src/chord-node.js → ChordNode                                │
│  Lógica do protocolo: join/leave, finger table, findSuccessor,│
│  put/get, catálogo de arquivos                                │
└───────────────┬───────────────────────────────────────────┘
                │ delega gravação/leitura de cópias
                ▼
┌────────────────────────────────────────────────────────────┐
│  src/replication.js → ReplicationManager                      │
│  Decide quais nós guardam cada arquivo (owner + N-1 sucessores)│
│  grava/lê as cópias e rebalanceia após entrada/saída de nós   │
└────────────────────────────────────────────────────────────┘

  src/ring.js   → matemática do anel (hash, intervalos circulares)
  src/logger.js → logs padronizados ([CHORD] [REPLICATION] [STORAGE] ...)
```

Cada nó guarda seus arquivos em disco, em
`data/node-<id>-<porta>/` (um diretório por nó), o que facilita mostrar
fisicamente onde cada réplica está armazenada.

## Como o anel funciona (`ring.js`)

O anel tem **32 posições**, numeradas de 1 a 32 (representação "pública";
internamente tudo é normalizado para `0..31` antes de qualquer conta e
convertido de volta).

- **`hashKey(nome)`** — aplica SHA-256 sobre o nome do arquivo e reduz o
  resultado para uma posição do anel (`1..32`). É o hash que decide "onde"
  um arquivo deveria morar.
- **`inInterval(id, start, end, incluiInício, incluiFim)`** — testa se um
  ponto está dentro de um intervalo **circular**. Ex.: `(30, 3]` contém
  `31, 32, 1, 2, 3`, porque o anel "dá a volta". É a base de todo o
  algoritmo do Chord, que nunca compara IDs de forma linear.
- **`add(id, offset)`** — soma com wraparound, usada para calcular o
  `start` de cada finger (`id + 2^i`).
- **`FINGER_COUNT = 5`** — como o anel tem `32 = 2⁵` posições, 5 fingers
  bastam para cobrir `log2(32)` saltos até qualquer nó.

## O nó Chord (`chord-node.js`)

Cada nó guarda `predecessor`, `successor` (a própria primeira entrada da
finger table, `fingers[0].node`), uma finger table com 5 entradas e um
diretório local de armazenamento.

### Entrada no anel — `join(bootstrap)`

1. Sem nó de bootstrap → o próprio nó cria o anel (`createRing`): vira
   predecessor e sucessor de si mesmo.
2. Com bootstrap → pergunta a esse nó "quem é sucessor do meu id?"
   (`POST /rpc/find-successor`), descobre o predecessor desse sucessor e se
   insere entre os dois, atualizando os ponteiros dos vizinhos via RPC.
3. Copia o catálogo de arquivos de um nó já existente (`adoptCatalogFrom`).
4. Reconstrói a própria finger table (`refreshFingerTable`) e dispara uma
   atualização em cadeia por todo o anel (`refresh-fingers`, nó a nó, fora
   da requisição HTTP atual via `setImmediate`, para o tempo de resposta não
   crescer com o tamanho do anel).
5. Chama `replicationManager.rebalanceAll()` — a chegada de um novo nó pode
   "roubar" a posse de algumas chaves, então é preciso realocar réplicas
   para manter exatamente o fator de replicação configurado.

### Saída — `leave()`

Reconecta predecessor ↔ sucessor diretamente (pulando o nó que sai),
propaga a atualização de fingers e pede ao sucessor para rodar
`POST /rpc/rebalance` (o sucessor pode ter que assumir os arquivos que o nó
que saiu guardava).

### Localizar o dono de uma chave — `findSuccessor(id)`

Algoritmo clássico do Chord:

1. Se `id` está no intervalo `(this.id, successor.id]`, o sucessor é a
   resposta.
2. Senão, `closestPrecedingFinger` escolhe o salto mais distante possível
   pela finger table, e a busca é repassada por RPC a esse nó (recursão
   distribuída, com contador `hops` para nunca ultrapassar 32 saltos).

### Armazenar e ler — `put`/`get`

- **`put(nome, conteúdo)`**: calcula o hash do nome → acha o `owner` via
  `findSuccessor` → delega a gravação real ao
  `ReplicationManager.replicate` → atualiza `catalogo.txt`, um arquivo
  especial replicado em **todos** os nós, usado para listar os arquivos
  existentes na rede.
- **`get(nome)`**: acha o owner e busca o conteúdo — localmente, se o nó
  atual for o owner, ou via `GET /rpc/files` remoto caso contrário. Pode ser
  chamado a partir de **qualquer** nó da rede, não só do owner.

## Estratégia de replicação (`replication.js`)

**Fator de replicação = 5** (`DEFAULT_REPLICATION_FACTOR`), com a estratégia
clássica de **replicação por lista de sucessores** ("successor-list
replication"): as cópias de um arquivo ficam no nó dono (owner) e nos
`N - 1` sucessores seguintes dele no anel.

- **`getReplicaNodes(owner)`** — anda pela cadeia de sucessores
  (`owner → sucessor(owner) → sucessor(sucessor(owner)) → ...`) até juntar
  `replicationFactor` nós distintos (para de crescer se o anel tiver menos
  nós que o fator configurado).
- **`replicate(nome, conteúdo, owner)`** — grava o conteúdo em cada um
  desses nós (local, se for o próprio processo, ou `PUT /rpc/files` via
  RPC), classificando o primeiro como `OWNER` e os demais como `REPLICA`, e
  loga quantas cópias foram confirmadas (`X/5`).
- **`rebalanceFile(nome)`** — depois de uma entrada/saída de nó, recalcula
  quem *deveria* guardar o arquivo (`desired`), descobre quem *realmente*
  guarda hoje (varrendo todos os nós vivos), copia o conteúdo para quem
  entrou na janela de réplicas e **apaga** dos nós que saíram dela. Garante
  sempre exatamente `N` cópias — nem mais, nem menos.
- **`rebalanceAll()`** — repete isso para todo arquivo do catálogo; é
  chamado automaticamente ao final de todo `join`/`leave`.

Por que essa estratégia? É a abordagem descrita no paper original do Chord
para tolerância a falhas: como as réplicas seguem o mesmo sentido de
navegação do anel (owner → sucessores), qualquer nó que já sabe localizar o
owner de uma chave também sabe, por construção, localizar suas réplicas —
não é necessário nenhum índice extra.

## Tolerância a falhas e recuperação do anel

O Chord "de livro" só reorganiza o anel quando alguém chama `join`/`leave`
explicitamente. Isso significa que se um nó **cai sem avisar a rede** — o
processo é fechado, o computador é desligado, a rede cai — os demais nós
continuam com `predecessor`/`successor`/finger table apontando para um nó
que não existe mais, e ninguém corrige isso sozinho. Encontramos esse
problema testando o sistema com vários nós e um deles caindo de forma
abrupta, com sintomas concretos:

1. **Acessar `catalogo.txt` travava e devolvia HTTP 504.** Mesmo o catálogo
   já estando fisicamente salvo em todos os nós (ele é replicado 100%, não só
   com fator 5), o `get()` sempre roteava pela DHT até um "dono" calculado —
   se esse roteamento passasse pelo nó morto, a requisição ficava presa até
   o timeout (10s por padrão) e devolvia 504, mesmo com a rede íntegra e o
   dado disponível localmente no nó que respondeu.
2. **Um arquivo cujo hash caísse exatamente na posição do nó morto não
   conseguia ser salvo nem lido**, porque a busca do sucessor insistia em
   contatar esse nó e recebia erro de conexão.
3. **Quando vários nós caíam juntos** (ex.: um computador com vários peers
   sendo desligado de uma vez, ou simplesmente fechando a aplicação), a rede
   parecia travar num loop — várias operações concorrentes (rebalanceamento,
   refresh de finger table, buscas) tentavam alcançar os mesmos nós mortos
   repetidamente, cada uma esperando o timeout inteiro de novo, e a lista de
   sucessores de reserva podia se esgotar cedo demais por disputa entre
   buscas concorrentes — o nó chegava a concluir (errado) que estava sozinho
   no anel.

### Correção 1 — o catálogo não depende mais de rotear pela DHT

Como `catalogo.txt` já é replicado em **todos** os nós (`updateCatalogOnAllNodes`),
`ChordNode.get()` passou a checar primeiro se o próprio nó já tem uma cópia
local (`hasLocal`) e, se tiver, devolve direto (`readLocal`) — sem nenhuma
chamada de rede. Qualquer nó que ainda está no ar sempre consegue responder
sobre o catálogo, independente de outros nós terem caído.

### Correção 2 — o anel se auto-repara quando percebe um nó morto

Adicionamos um mecanismo reativo de detecção e correção de falha (o projeto
não tinha nenhum antes):

- **`successorList`** — cada nó guarda os próximos sucessores vivos
  conhecidos (reaproveita `ReplicationManager.getReplicaNodes`, a mesma
  lógica da replicação), atualizada a cada `join` e a cada vez que o
  `refresh-fingers` percorre o anel. É o "plano B" de rota.
- **`ensureLiveSuccessor()` / `_healSuccessor()`** — quando um nó percebe que
  seu sucessor imediato não responde, remove-o da lista, tenta o próximo
  vivo conhecido, e repete até confirmar um que responda de verdade (ou
  esgotar a lista). Ao encontrar um substituto, avisa-o (`PUT
  /rpc/predecessor`), atualiza a própria finger table e dispara
  `refresh-fingers` + `rebalanceAll` em segundo plano — o vizinho aplicando
  uma "saída forçada" no nó que sumiu, em vez de esperar por um `/leave` que
  nunca vai chegar.
- **`findSuccessor`** ganhou duas fases: a resposta final passa por
  `ensureLiveSuccessor()` antes de ser devolvida, e `routeFindSuccessor()`
  tenta a finger mais distante, cai para as próximas e, se o próprio
  sucessor estiver morto, aciona a mesma cura — em vez de estourar o timeout
  e falhar a busca inteira.
- **`getAllNodes()`** (usada pelo broadcast do catálogo e pelo
  rebalanceamento) e **`getReplicaNodes()`** (usada pela replicação e pela
  `successorList`) passaram a parar o percurso e seguir com o que já
  confirmaram, em vez de lançar exceção assim que um nó no meio do caminho
  não responde.

O resultado é que uma chave cujo dono morreu passa a ser automaticamente
reatribuída ao próximo nó vivo do anel, com os ponteiros do anel
efetivamente corrigidos — não é um "tentar de novo" pontual.

### Correção 3 — vários nós caindo juntos não trava mais a rede num loop

Testando o cenário de um computador com **vários peers** sendo desligado de
uma vez (não só um nó isolado), apareceram dois problemas novos:

- Cada operação que tentava alcançar um dos nós mortos esperava o timeout
  inteiro (10s) de novo, mesmo que aquele nó já tivesse falhado segundos
  antes em outra chamada — e como várias operações concorrentes faziam isso
  ao mesmo tempo, os timeouts se empilhavam e a rede parecia travada.
- As 5 buscas concorrentes disparadas por `refreshFingerTable()` cada uma
  tentava corrigir o sucessor morto por conta própria, disputando a mesma
  `successorList` ao mesmo tempo — uma promoção "roubava" o candidato que
  outra também ia usar, esvaziando a lista cedo demais e fazendo o nó
  concluir (errado) que estava sozinho no anel.

Duas mudanças resolveram isso:

- **Cache de nós mortos** (`ChordNode.deadNodes`, expira em 15s) — depois da
  primeira falha real contra um nó, `rpc()` passa a rejeitar novas tentativas
  contra ele **na hora**, sem novo timeout, até o prazo expirar. Se o nó
  responder de novo a qualquer momento, ele sai do cache imediatamente.
- **Cura do sucessor centralizada** — `ensureLiveSuccessor()` agora só deixa
  **uma** correção rodar por vez; se várias buscas descobrem a falha ao
  mesmo tempo, todas aguardam e reaproveitam o mesmo resultado em vez de
  mexer na `successorList` cada uma por conta própria. O mesmo vale para o
  rebalanceamento: `ReplicationManager.rebalanceAll()` reaproveita uma
  execução já em andamento em vez de disparar varreduras duplicadas pela
  rede quando mais de um nó percebe a falha ao mesmo tempo.

Mesmo assim, restava um caso: quando **todos** os nós da `successorList`
caíam juntos (mais nós mortos do que o fator de replicação cobre), o nó
concluía — errado — que estava sozinho no anel, mesmo havendo outros nós
vivos mais longe. A correção final foi consultar a **finger table** (que
enxerga muito mais longe que a lista de reserva) antes de desistir —
`discoverLiveSuccessorViaFingers()`. A primeira versão dessa consulta pedia
para o nó distante resolver a busca sozinho, o que podia gerar um impasse
(esse nó distante às vezes precisa voltar a perguntar justamente ao nó que
está travado esperando a correção); a versão final faz uma checagem local e
direta, sem esse risco.

### Correção 4 — detecção contínua, sem depender de tráfego

Todas as correções acima só aconteciam quando alguma operação (`put`,
`get`, etc.) de fato tentava rotear pelo nó morto. Se ninguém usasse a rede
naquele momento, um nó já caído continuava aparecendo normalmente para os
demais, inclusive na tela.

- **`stabilizeTick()`**, ligado por `startStabilization()` a cada entrada
  na rede (`createRing`/`join`) e desligado em `leave()` — roda a cada
  poucos segundos, em segundo plano, por nó. Confirma o sucessor (curando
  via `ensureLiveSuccessor()` se preciso), limpa o predecessor se ele parou
  de responder, e atualiza a finger table.

Com isso, um nó que caiu passa a sumir da rede e da tela sozinho, mesmo sem
nenhum upload/download acontecendo.

## Camada HTTP / RPC (`node-server.js` e `server.js`)

- **`node-server.js`** — servidor HTTP de cada `ChordNode`. Expõe:
  - API pública: `GET/POST /api/files`, `GET /api/state`, `POST /join`,
    `POST /leave`;
  - RPC interno (nó fala com nó): `/rpc/find-successor`,
    `/rpc/successor`, `/rpc/predecessor`, `/rpc/refresh-fingers`,
    `/rpc/files`, `/rpc/files/exists`, `/rpc/catalog`, `/rpc/rebalance`.
- **`server.js`** — "painel controlador" na porta `5000`. Sobe/derruba
  instâncias de `ChordNode` dentro do mesmo processo Node (prático para
  simular vários nós numa única máquina) e serve o painel web
  (`public/manager.html`) que lista os nós ativos.
- Comunicação entre nós usa `node.rpc(alvo, caminho, opções)` — um `fetch`
  com timeout configurável (`requestTimeout`, padrão 10s) e
  `AbortController`.

## Executar o projeto

Inicie o painel controlador, que utiliza a porta `5000`:

```bash
npm start
```

Abra `http://127.0.0.1:5000`. Na página, informe o ID, IP e porta do novo
nó. A porta `5000` fica reservada ao painel; utilize `5001`, `5002`, `5003`
etc.

Para o primeiro nó, por exemplo:

- ID: `8`
- IP: `127.0.0.1`
- Porta de início: `5001`
- Opção: **Criar um novo anel**

Para o segundo nó, informe seu próprio ID/IP/porta (`20`, `127.0.0.1`,
`5002`) e selecione **Entrar por um nó existente**. Como destino, informe o
nó 8 em `127.0.0.1:5001`.

O painel controlador lista todos os nós locais em execução. Use **Abrir
painel** para ver o predecessor, sucessor, anel e finger table de um nó
específico. Por exemplo, o painel do nó na porta `5001` estará em
`http://127.0.0.1:5001`, e seu estado JSON em
`http://127.0.0.1:5001/api/state`.

Na mesma máquina, os nós compartilham o IP `127.0.0.1`, mas obrigatoriamente
usam portas diferentes.

### Executar em máquinas da rede local

Em cada máquina, execute `npm start` e abra o painel usando o IP da própria
máquina, por exemplo `http://172.16.1.10:5000`. O servidor escuta em todas
as interfaces e o painel preenche automaticamente o campo **IP desta
máquina**.

Na primeira máquina, crie um novo anel. Nas demais, escolha **Entrar por um
nó existente** e informe o ID, IP e porta do primeiro nó (por exemplo,
`1`, `172.16.1.10`, `5001`). Cada nó deve anunciar o IP `172.16.X.X` da
máquina em que está executando, nunca `127.0.0.1` ou `0.0.0.0`.

Libere no firewall TCP a porta `5000` para o painel e as portas usadas
pelos nós (`5001`, `5002` etc.). Confirme a comunicação de outra máquina
com:

```bash
curl http://172.16.1.10:5001/api/state
```

## Arquivos: `put` e `get` via API

Cada `ChordNode` oferece `put(nome, conteúdo)` e `get(nome)`. O SHA-256 do
nome é convertido para uma posição entre 1 e 32; `findSuccessor` escolhe o
primeiro nó ativo nessa posição ou depois dela. Assim, posições sem nó são
naturalmente armazenadas no próximo nó ativo do anel.

```js
await node.put('trabalho.txt', Buffer.from('conteúdo'));
const arquivo = await node.get('trabalho.txt');
console.log(arquivo.content.toString());
```

Todo `put` também atualiza `catalogo.txt` (um nome por linha). O catálogo
usa o mesmo hash e é armazenado na própria rede. Pela API HTTP de qualquer
nó:

```bash
curl -X POST http://127.0.0.1:5001/api/files \
  -H 'content-type: application/json' \
  -d '{"name":"trabalho.txt","content":"conteúdo"}'

curl -OJ 'http://127.0.0.1:5001/api/files?name=trabalho.txt'
curl 'http://127.0.0.1:5001/api/files?name=catalogo.txt'
```

Para bytes arbitrários, envie `content` em Base64 e acrescente
`"encoding":"base64"` ao JSON do `POST`.

## Testes automatizados

```bash
npm test
```

Cobre a matemática do anel (`ring.js`) e um cenário de ponta a ponta via
HTTP (subida de nós, `put`, replicação e `get` a partir de uma posição sem
nó dono direto).

---

## Guia da apresentação (26/08/2026)

Roteiro para a demonstração ao vivo, cobrindo os quatro pontos pedidos:
**(1)** estratégia de replicação, **(2)** inserção de arquivo, **(3)**
recuperação de arquivo, **(4)** exibição das réplicas.

### Preparação (fazer com antecedência, não no dia)

1. Confirme a versão do Node: `node --version` (precisa ser ≥ 18).
2. Rode `npm test` e confirme que os 8 testes passam.
3. Se for demonstrar em várias máquinas, teste a conectividade entre elas
   com `curl http://IP:PORTA/api/state` **antes** da apresentação, e libere
   as portas no firewall. Se o Wi-Fi da sala for instável, prefira
   demonstrar tudo em uma única máquina (o painel já sobe vários nós no
   mesmo processo).
4. Apague a pasta `data/` antes do ensaio final para começar do zero:
   `rm -rf data` (Git Bash) ou `Remove-Item -Recurse -Force data`
   (PowerShell).
5. Deixe um terminal grande e visível rodando `npm start` — os logs
   `[CHORD]`, `[REPLICATION]` e `[STORAGE]` aparecem ali em tempo real e são
   sua principal evidência visual de replicação.
6. Garanta que todas as máquinas envolvidas estão com o código mais
   recente (`git pull origin main`) antes de subir os nós — versões
   diferentes rodando ao mesmo tempo causam comportamento inconsistente.

### Roteiro sugerido (≈5-8 nós)

Com o fator de replicação fixo em 5, use pelo menos 6 nós para que dê para
mostrar réplicas espalhadas por nós diferentes claramente. Sugestão de IDs
espaçados no anel de 1 a 32: `1, 6, 12, 18, 24, 30`.

1. **Suba o painel**: `npm start`, abra `http://127.0.0.1:5000`.
2. **Crie o primeiro nó** (ID `1`, porta `5001`, "Criar um novo anel").
3. **Adicione os demais** (`6/5002`, `12/5003`, `18/5004`, `24/5005`,
   `30/5006`), cada um "Entrando" pelo nó `1` em `127.0.0.1:5001`. Mostre no
   painel controlador a lista de nós com predecessor/sucessor de cada um —
   já demonstra o anel formado.
4. **(1) Estratégia de replicação** — abra o painel de qualquer nó e, ao
   vivo, explique com apoio no terminal: "cada arquivo é gravado no nó dono
   (definido pelo hash SHA-256 do nome) e replicado nos 4 sucessores
   seguintes dele no anel — fator de replicação 5, definido em
   `src/replication.js`". Pode mostrar rapidamente o trecho
   `getReplicaNodes` no editor.
5. **(2) Inserção de arquivo** — no painel de um nó (ex.: nó `1`,
   `http://127.0.0.1:5001`), use o formulário de upload para enviar um
   arquivo de teste. A mensagem de sucesso mostra
   `Arquivo armazenado pelo nó X (hash Y)`. No terminal do `npm start`,
   aponte o bloco de log que mostra o dono e as 4 réplicas sendo
   confirmadas em tempo real.
6. **(3) Recuperação de arquivo** — abra o painel de um nó **diferente** do
   dono e baixe o mesmo arquivo pela lista de catálogo. Isso prova que a
   rede é P2P de verdade: qualquer nó localiza e entrega o arquivo, não só
   quem o guarda.
7. **(4) Exibição das réplicas** — três formas de mostrar, use pelo menos
   duas:
   - **Log do terminal** (passo 5 acima) — já mostra os IDs dos nós.
   - **Sistema de arquivos**: abra `data/` e mostre que o arquivo existe
     fisicamente em `node-18-5004/`, `node-24-5005/` etc.
   - **Via API**, consultando cada nó se ele guarda o arquivo
     (`GET /rpc/files/exists?name=...`).
8. **Bônus — saída graciosa / rebalanceamento**: remova um nó que guarda
   uma réplica (botão "Remover" no painel, ou `DELETE /api/nodes/<porta>`)
   e mostre no terminal o log de rebalanceamento, provando que a rede
   recompõe as 5 cópias automaticamente entre os nós restantes.
9. **Bônus avançado — falha abrupta de um nó** (ver
   [Tolerância a falhas e recuperação do anel](#tolerância-a-falhas-e-recuperação-do-anel)):
   diferente do passo 8 (saída avisada, via `/leave`), aqui um nó
   simplesmente some sem avisar ninguém — o cenário real de "o computador de
   alguém do grupo desligou". Se a demo for em várias máquinas, é só fechar
   o terminal de uma delas no meio da apresentação; em poucos segundos os
   demais percebem sozinhos (mesmo sem nenhum upload/download acontecendo)
   e o upload/download seguinte continua funcionando normalmente. Se a demo
   for numa máquina só, isso não dá pra simular derrubando um nó pelo
   painel — todos os nós locais rodam dentro do mesmo processo do
   `npm start`, então matar o processo derruba todos de uma vez. **Ensaie
   esse passo com antecedência** para decidir se ele entra na apresentação.

### Coisas a checar no dia

- Terminal com fonte grande o bastante para o log ser lido da plateia.
- Ter um arquivo de teste pequeno pronto (ex.: um `.txt` de poucas linhas)
  para não perder tempo escolhendo arquivo na hora.
- Ensaiar a ordem de janelas/abas: painel controlador → painel do nó de
  upload → terminal de logs → painel do nó de download → pasta `data/`.
- Ter um plano B sem rede (tudo em `127.0.0.1`) caso o Wi-Fi da sala falhe.
