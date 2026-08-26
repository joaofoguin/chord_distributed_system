# Chord com Replicação — Apresentação

Guia para a demonstração ao vivo: o que o sistema faz, como as estratégias
foram desenvolvidas e o roteiro para mostrar tudo funcionando.

## O que o sistema entrega

Uma rede Chord (anel P2P, 32 posições) onde qualquer nó pode receber um
arquivo, encontrar automaticamente quem deve guardá-lo, replicar esse
arquivo em outros nós por segurança, e devolvê-lo quando qualquer outro nó
da rede pedir — inclusive se um dos nós que guarda o arquivo já não estiver
mais disponível.

## Estratégia de replicação

Cada arquivo tem, além do nó "dono" (definido pelo hash do nome do
arquivo), 4 cópias extras guardadas nos 4 nós seguintes a ele no anel — um
total de 5 cópias por arquivo, sempre.

Escolhemos essa estratégia (réplicas nos sucessores seguintes do dono, e
não em nós aleatórios) porque qualquer nó que já sabe localizar o dono de
um arquivo automaticamente já sabe onde estão as réplicas — não precisa de
nenhuma estrutura extra de controle.

Sempre que um nó entra ou sai da rede, o sistema recalcula quem deveria
guardar cada arquivo e ajusta as cópias sozinho — copia para quem entrou na
janela de 5 nós e remove de quem saiu dela. Isso garante que a rede nunca
fica com menos de 5 cópias (nem guarda cópias a mais) depois de qualquer
mudança na topologia.

Toda essa lógica fica em `ReplicationManager`, em `src/replication.js`:
`getReplicaNodes()` calcula o dono + os 4 sucessores para um arquivo,
`replicate()` grava as 5 cópias, e `rebalanceFile()`/`rebalanceAll()` fazem
o ajuste automático após qualquer entrada ou saída de nó.

## Desafios enfrentados durante o desenvolvimento

O Chord "de livro" só reorganiza o anel quando um nó sai educadamente,
avisando a rede. Ao testar com vários nós reais em máquinas diferentes, e
não só em ambiente controlado, fomos esbarrando em problemas que só
aparecem quando um nó desaparece de verdade — sem avisar ninguém. Cada um
deles foi resolvido em `ChordNode`, na classe principal do protocolo
(`src/chord-node.js`):

- **Problema:** um nó que caía sem avisar (computador desligado, processo
  encerrado) deixava os vizinhos apontando para um endereço morto — e toda
  busca que passasse por ali travava até estourar o tempo de espera e
  falhar.
  **Solução:** além do sucessor imediato, cada nó passou a guardar uma
  pequena lista de reserva com os próximos nós vivos que conhece
  (`successorList`). Ao perceber que o vizinho não responde mais, ele
  promove sozinho o próximo da lista como novo sucessor —
  `ensureLiveSuccessor()` / `_healSuccessor()`.

- **Problema:** mesmo com o catálogo de arquivos guardado em todos os nós,
  consultá-lo dependia de rotear pela rede até um "dono" calculado — se
  essa rota passasse por um nó morto, a consulta travava mesmo o dado já
  estando disponível ali mesmo, localmente.
  **Solução:** o método `get()` passou a checar primeiro se o próprio nó já
  tem uma cópia local do catálogo e responder direto do disco, sem
  depender de alcançar nenhum outro nó.

- **Problema:** quando vários nós caíam juntos (ex.: um computador inteiro
  com vários peers desligando de uma vez), várias operações da rede
  tentavam alcançar os mesmos nós mortos ao mesmo tempo, cada uma esperando
  o tempo de espera inteiro de novo — a rede parecia travada.
  **Solução:** depois de uma falha real, o nó fica marcado como
  indisponível por um tempo curto (`deadNodes`, dentro do método `rpc()`);
  novas tentativas contra ele falham na hora em vez de esperar de novo.

- **Problema:** ainda no cenário de vários nós caindo juntos, se a lista de
  reserva se esgotasse inteira (todos os candidatos dela também mortos), o
  nó concluía — errado — que estava sozinho no anel, mesmo havendo outros
  nós vivos mais longe.
  **Solução:** antes de desistir, o nó consulta sua finger table (tabela de
  rotas, que enxerga muito mais longe que a lista de reserva) para
  encontrar outro nó vivo e se reconectar a ele —
  `discoverLiveSuccessorViaFingers()`.

- **Problema:** a primeira versão dessa consulta pedia para o nó distante
  resolver a busca sozinho — e ele podia legitimamente precisar voltar a
  perguntar para o próprio nó que estava travado esperando essa correção,
  um impasse que só se resolvia no tempo de espera máximo.
  **Solução:** a consulta virou uma checagem direta e local (o candidato só
  responde se está vivo, sem precisar rotear nada), eliminando o impasse.

- **Problema:** todas essas correções só aconteciam quando alguma operação
  (upload ou download) forçava uma busca pela rede — se ninguém usasse a
  rede naquele momento, um nó já desconectado continuava aparecendo
  normalmente para os demais.
  **Solução:** cada nó passou a verificar sozinho, em segundo plano e a
  cada poucos segundos, se seus vizinhos ainda respondem —
  `stabilizeTick()`, ligado automaticamente ao entrar na rede.

- **Problema:** quando várias buscas percebiam a mesma falha ao mesmo
  tempo, cada uma tentava corrigir o sucessor por conta própria, disputando
  a mesma lista de reserva e esvaziando-a mais rápido do que deveria.
  **Solução:** só uma correção roda por vez por nó; buscas concorrentes
  aguardam e reaproveitam o mesmo resultado, em vez de competir entre si.

## Como o sistema é organizado

| Arquivo | Responsabilidade |
|---|---|
| `src/ring.js` | Matemática do anel: hash dos nomes de arquivo, aritmética circular de posições |
| `src/chord-node.js` | Protocolo Chord: entrada/saída de nós, localização de chaves, upload/download, tolerância a falhas |
| `src/replication.js` | Decide quais nós guardam cada arquivo, grava as cópias e rebalanceia |
| `src/node-server.js` | API HTTP de cada nó (upload, download, comunicação entre nós) |
| `src/server.js` | Painel controlador — sobe/derruba nós e mostra o estado da rede |

## Como executar

```bash
npm start
```

Abre o painel em `http://127.0.0.1:5000`. A partir dele, criamos cada nó
informando ID, IP e porta — o primeiro cria a rede, os demais entram
apontando para um nó já existente.

## Roteiro da demonstração ao vivo

1. **Subir a rede** — criar 6 nós pelo painel, o primeiro criando o anel e
   os demais entrando por ele. O painel já mostra o anel formado, com
   predecessor e sucessor de cada nó.

2. **Inserir um arquivo** — usar o formulário de upload no painel de
   qualquer nó. A resposta mostra em qual nó o arquivo foi armazenado
   (dono) e o hash calculado. O terminal mostra em tempo real a
   distribuição das 5 cópias pela rede.

3. **Mostrar onde as réplicas ficaram** — três formas, complementares:
   - o log no terminal;
   - a pasta `data/`, mostrando o arquivo fisicamente presente em cada
     diretório de nó;
   - consultando cada nó pela API se ele guarda aquele arquivo.

4. **Recuperar o arquivo por outro nó** — abrir o painel de um nó
   diferente do dono e baixar o mesmo arquivo pela lista do catálogo.
   Mostra que a busca funciona a partir de qualquer ponto da rede, não só
   de quem guarda o arquivo.

5. **(Opcional) Tolerância a falhas** — encerrar um nó abruptamente (ou,
   numa demonstração em várias máquinas, desligar o computador de um dos
   integrantes) e mostrar que a rede continua respondendo: o catálogo segue
   acessível, um novo upload/download funciona normalmente, e o nó
   derrubado some sozinho da tela dos demais em poucos segundos.
