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
arquivo), **4 cópias extras** guardadas nos 4 nós seguintes dele no anel —
um total de **5 cópias por arquivo**, sempre.

```
arquivo.txt → hash → Nó 18 (dono)
                       ├─ cópia no Nó 18 (dono)
                       ├─ cópia no Nó 24 (sucessor 1)
                       ├─ cópia no Nó 30 (sucessor 2)
                       ├─ cópia no Nó 1  (sucessor 3)
                       └─ cópia no Nó 6  (sucessor 4)
```

Escolhemos essa estratégia (réplicas nos sucessores seguintes do dono, e
não em nós aleatórios) porque qualquer nó que já sabe localizar o dono de
um arquivo, automaticamente já sabe onde estão as réplicas — não precisa de
nenhuma estrutura extra de controle.

Sempre que um nó entra ou sai da rede, o sistema recalcula quem deveria
guardar cada arquivo e ajusta as cópias sozinho — copia para quem entrou na
janela de 5 nós e remove de quem saiu dela. Isso garante que a rede nunca
fica com menos de 5 cópias (nem guarda cópias a mais) depois de qualquer
mudança na topologia.

## Tolerância a falhas

Além do rebalanceamento (quando um nó sai avisando a rede), o sistema
também lida com um nó que **desaparece sem avisar** — computador desligado,
processo encerrado, rede caindo. Nesse caso:

1. O nó vizinho percebe que o sucessor não responde mais.
2. Ele já mantém uma lista dos próximos nós vivos conhecidos e promove o
   próximo da lista como novo sucessor.
3. Avisa esse novo sucessor, atualiza sua tabela de rotas e reorganiza as
   réplicas em segundo plano.

Na prática, isso significa que arquivos e o catálogo continuam acessíveis
mesmo que um dos nós da rede caia no meio da demonstração.

## Como o sistema é organizado

| Arquivo | Responsabilidade |
|---|---|
| `src/ring.js` | Matemática do anel: hash dos nomes de arquivo, aritmética circular de posições |
| `src/chord-node.js` | Protocolo Chord: entrada/saída de nós, localização de chaves, upload/download |
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

1. **Subir a rede** — criar 6 nós pelo painel (ex.: IDs `1, 6, 12, 18, 24,
   30`, portas `5001` a `5006`), o primeiro criando o anel e os demais
   entrando por ele. O painel já mostra o anel formado, com predecessor e
   sucessor de cada nó.

2. **Inserir um arquivo** — usar o formulário de upload no painel de
   qualquer nó. A resposta mostra em qual nó o arquivo foi armazenado
   (dono) e o hash calculado. No terminal, o log mostra a distribuição
   completa:

   ```
   [REPLICATION] Owner: Node 18
   [REPLICATION] Nós selecionados: 18 → 24 → 30 → 1 → 6
   [REPLICATION] Cópias confirmadas: 5/5
   ```

3. **Mostrar onde as réplicas ficaram** — três formas, complementares:
   - o log acima, no terminal;
   - a pasta `data/`, mostrando o arquivo fisicamente presente em cada
     diretório de nó (`data/node-18-5004/`, `data/node-24-5005/`, etc.);
   - consultando cada nó pela API:
     ```bash
     curl "http://127.0.0.1:5001/rpc/files/exists?name=arquivo.txt"
     ```

4. **Recuperar o arquivo por outro nó** — abrir o painel de um nó
   **diferente** do dono e baixar o mesmo arquivo pela lista do catálogo.
   Mostra que a busca funciona a partir de qualquer ponto da rede, não só
   de quem guarda o arquivo.

5. **(Opcional) Tolerância a falhas** — encerrar um nó abruptamente (ou,
   numa demonstração em várias máquinas, desligar o computador de um dos
   integrantes) e mostrar que a rede continua respondendo: o catálogo segue
   acessível e um novo upload/download funciona normalmente, com o sistema
   reorganizando o anel sozinho.
