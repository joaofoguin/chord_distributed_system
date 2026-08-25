"use strict";

const fs = require("node:fs/promises");
const path = require("node:path");
const {
  FINGER_COUNT,
  add,
  hashKey,
  inInterval,
  validateId,
} = require("./ring");

const {
  ReplicationManager,
  DEFAULT_REPLICATION_FACTOR,
} = require("./replication");

const {
  section,
  chord,
  replication,
  storage,
  network,
  error,
} = require("./logger");

const CATALOG_NAME = "catalogo.txt";

class ChordNode {
  constructor({
    id,
    host = "127.0.0.1",
    port = 5000,
    requestTimeout = 10000,
    storageDirectory,
  } = {}) {
    this.id = validateId(id);
    this.host = String(host || "").trim();
    if (!this.host || this.host === "0.0.0.0" || this.host === "::") {
      throw new Error(
        "Informe o IP ou hostname pelo qual os outros nós acessam esta máquina",
      );
    }
    this.port = Number(port);
    if (!Number.isInteger(this.port) || this.port < 1 || this.port > 65535) {
      throw new Error("A porta deve ser um inteiro entre 1 e 65535");
    }
    this.requestTimeout = requestTimeout;
    this.storageDirectory =
      storageDirectory ||
      path.join(process.cwd(), "data", `node-${this.id}-${this.port}`);
    this.enableLookupLogs = false;
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    this.joined = false;
    // Cópia dos próximos sucessores vivos no momento da última atualização do
    // anel (join/leave). Serve de plano B quando o sucessor imediato some sem
    // avisar a rede (ex.: computador desligado) — ver promoteNextSuccessor.
    this.successorList = [];
    // Nós que falharam recentemente (timeout/conexão recusada). Enquanto um
    // id estiver aqui, novas chamadas a ele falham na hora em vez de esperar
    // o timeout inteiro de novo — evita que várias operações concorrentes
    // (rebalanceamento, refresh de fingers, buscas) fiquem cada uma na sua
    // vez tentando de novo o mesmo nó morto por 10s.
    this.deadNodes = new Map();
    this.deadNodeCooldownMs = 15000;

    this.replicationManager = new ReplicationManager(
      this,
      DEFAULT_REPLICATION_FACTOR,
    );
  }

  get reference() {
    return { id: this.id, host: this.host, port: this.port };
  }

  buildEmptyFingerTable() {
    return Array.from({ length: FINGER_COUNT }, (_, index) => ({
      index: index + 1,
      start: add(this.id, 2 ** index),
      node: null,
    }));
  }

  get successor() {
    return this.fingers[0].node;
  }

  set successor(node) {
    this.fingers[0].node = node;
  }

  createRing() {
    this.predecessor = this.reference;
    for (const finger of this.fingers) finger.node = this.reference;
    this.successorList = [this.reference];
    this.joined = true;
  }

  async join(bootstrap) {
    chord(
      `Node ${this.id} iniciando entrada na rede${
        bootstrap ? ` através do Node ${bootstrap.id}` : ""
      }`,
    );

    if (this.joined) throw new Error("Este nó já pertence a uma rede Chord");
    if (!bootstrap) {
      this.createRing();
      return this.state();
    }

    const contact = normalizeReference(bootstrap);
    if (contact.id === this.id)
      throw new Error("O nó de entrada não pode ter o mesmo id");

    // Localiza a posição do novo nó no anel usando o nó de entrada.
    const successor = await this.rpc(contact, "/rpc/find-successor", {
      method: "POST",
      body: { id: this.id },
    });
    if (successor.id === this.id)
      throw new Error(`O id ${this.id} já está em uso`);

    const predecessorResult = await this.rpc(successor, "/rpc/predecessor");
    const predecessor = predecessorResult.node || successor;

    this.successor = successor;
    this.predecessor = predecessor;

    // Faz o novo nó entrar entre predecessor e sucessor.
    await this.rpc(successor, "/rpc/predecessor", {
      method: "PUT",
      body: { node: this.reference },
    });
    if (predecessor.id !== successor.id) {
      await this.rpc(predecessor, "/rpc/successor", {
        method: "PUT",
        body: { node: this.reference },
      });
    } else {
      // A rede possuía apenas um nó.
      await this.rpc(successor, "/rpc/successor", {
        method: "PUT",
        body: { node: this.reference },
      });
    }

    this.joined = true;

    chord(
      `Node ${this.id} entrou na rede | predecessor=${this.predecessor.id} | successor=${this.successor.id}`,
    );

    // Busca uma cópia física do catálogo com um nó que já pertencia ao anel,
    // para que este nó possa listar arquivos mesmo se vier a ser o dono do catálogo.
    await this.adoptCatalogFrom(successor);

    await this.refreshFingerTable();
    await this.refreshSuccessorList();

    // A entrada altera também as fingers dos nós que já estavam no anel.
    await this.rpc(this.successor, "/rpc/refresh-fingers", {
      method: "POST",
      body: { originId: this.id, hops: 0 },
    });

    // Garante que, após a entrada, somente `replicationFactor` nós guardem
    // cada arquivo (o nó novo pode assumir cópias que saem da janela de réplicas).
    try {
      await this.replicationManager.rebalanceAll();
    } catch (rebalanceError) {
      replication(
        `Falha ao rebalancear réplicas após entrada do Node ${this.id}: ${rebalanceError.message}`,
      );
    }

    return this.state();
  }

  /** Saída graciosa: reconecta os vizinhos e restabelece o fator de replicação. */
  async leave() {
    this.assertJoined();
    section("[CHORD] SAÍDA DE NÓ");
    chord(`Node ${this.id} saindo da rede`);

    const successor = this.successor;
    const predecessor = this.predecessor;

    if (!successor || successor.id === this.id) {
      this.joined = false;
      this.predecessor = null;
      for (const finger of this.fingers) finger.node = null;
      chord(`Node ${this.id} era o único nó da rede.`);
      return { ok: true };
    }

    // Reconecta predecessor e sucessor diretamente, removendo este nó do anel.
    await this.rpc(predecessor, "/rpc/successor", {
      method: "PUT",
      body: { node: successor },
    });
    await this.rpc(successor, "/rpc/predecessor", {
      method: "PUT",
      body: { node: predecessor },
    });

    this.joined = false;

    chord(
      `Node ${this.id} saiu | Node ${predecessor.id} agora aponta para Node ${successor.id}`,
    );

    try {
      await this.rpc(successor, "/rpc/refresh-fingers", {
        method: "POST",
        body: { originId: successor.id, hops: 0 },
      });
    } catch (refreshError) {
      network(
        `Falha ao atualizar finger tables após saída do Node ${this.id}: ${refreshError.message}`,
      );
    }

    // O restante do anel precisa recompor as cópias que este nó guardava.
    try {
      await this.rpc(successor, "/rpc/rebalance", { method: "POST" });
    } catch (rebalanceError) {
      replication(
        `Falha ao rebalancear réplicas após saída do Node ${this.id}: ${rebalanceError.message}`,
      );
    }

    return { ok: true };
  }

  /** Copia o catálogo de um nó que já pertence ao anel para o armazenamento local. */
  async adoptCatalogFrom(source) {
    try {
      const result = await this.rpc(
        source,
        `/rpc/files?name=${encodeURIComponent(CATALOG_NAME)}`,
      );
      await this.storeLocal(CATALOG_NAME, Buffer.from(result.content, "base64"));
    } catch (adoptError) {
      if (
        adoptError.code !== "ENOENT" &&
        !/não encontrado/i.test(adoptError.message)
      ) {
        network(`Não foi possível obter o catálogo ao entrar: ${adoptError.message}`);
      }
    }
  }

  async updateCatalogOnAllNodes(content) {
    const nodes = await this.getAllNodes();

    const results = [];

    for (const target of nodes) {
      try {
        if (target.id === this.id) {
          await this.storeLocal(CATALOG_NAME, content);
        } else {
          await this.rpc(target, "/rpc/catalog", {
            method: "PUT",
            body: {
              content: content.toString("base64"),
            },
          });
        }

        results.push({
          id: target.id,
          status: "ONLINE",
        });
      } catch (error) {
        results.push({
          id: target.id,
          status: "FAILED",
          error: error.message,
        });
      }
    }

    return results;
  }

  async getAllNodes() {
    this.assertJoined();

    const nodes = [];
    const visited = new Set();

    let current = this.reference;

    while (current && !visited.has(current.id)) {
      visited.add(current.id);
      nodes.push(current);

      if (current.id === this.id) {
        current = this.successor;
        continue;
      }

      try {
        const result = await this.rpc(current, "/rpc/successor");
        current = normalizeReference(result.node);
      } catch (rpcError) {
        // Um nó inacessível no meio do anel não pode travar o catálogo nem
        // o rebalanceamento para o resto da rede — encerra o percurso aqui
        // e segue com os nós que já foram confirmados como vivos.
        network(
          `Node ${current.id} inacessível ao percorrer o anel: ${rpcError.message}`,
        );
        break;
      }
    }

    return nodes;
  }

  async refreshFingerTable() {
    const nodes = await Promise.all(
      this.fingers.map((finger) => this.findSuccessor(finger.start)),
    );
    this.fingers.forEach((finger, index) => {
      finger.node = nodes[index];
    });
  }

  async refreshRingFingerTables(originId, hops = 0) {
    validateId(originId);
    // Só interrompe ao completar uma volta (hops > 0); isso permite usar o
    // próprio nó de partida como origem (necessário após uma saída de nó).
    if (hops > 0 && this.id === Number(originId)) return { ok: true };
    if (hops >= 32)
      throw new Error("Limite de nós excedido ao atualizar finger tables");

    await this.refreshFingerTable();
    await this.refreshSuccessorList();

    // Cada nó responde após atualizar a própria tabela. O próximo salto ocorre
    // fora da requisição atual para o tempo total não crescer com o anel.
    const next = this.successor;
    setImmediate(() => {
      this.rpc(next, "/rpc/refresh-fingers", {
        method: "POST",
        body: { originId: Number(originId), hops: hops + 1 },
      }).catch((error) => {
        console.error(
          `Não foi possível atualizar as fingers após o nó ${this.id}: ${error.message}`,
        );
      });
    });
    return { ok: true };
  }

  async findSuccessor(rawId, hops = 0, options = {}) {
    const id = validateId(rawId);
    const logLookup = options.log === true;

    if (hops === 0 && logLookup) {
      chord(`Localizando sucessor para hash ${id} a partir do Node ${this.id}`);
    }
    if (!this.joined || !this.successor)
      throw new Error("O nó ainda não entrou em uma rede");
    if (this.successor.id === this.id) return this.reference;
    if (id === this.id) return this.reference;

    if (inInterval(id, this.id, this.successor.id, false, true)) {
      // Antes de responder, garante que o sucessor apontado ainda está vivo;
      // se não estiver, promove o próximo sucessor conhecido e repara o
      // anel, para o pedido não falhar por causa de um nó que já caiu.
      const owner = await this.ensureLiveSuccessor();

      if (logLookup) {
        chord(
          `Hash ${id} encontrado entre Node ${this.id} e Node ${owner.id} → sucessor Node ${owner.id}`,
        );
      }

      return owner;
    }

    if (hops >= 32)
      throw new Error("Limite de saltos excedido ao procurar sucessor");

    return this.routeFindSuccessor(id, hops, logLookup);
  }

  /**
   * Tenta encaminhar a busca pela finger table; se o nó escolhido estiver
   * inacessível, tenta as próximas fingers e por fim o sucessor (já curado,
   * se preciso) — permite que a rede continue respondendo mesmo quando um
   * ou mais nós saíram sem avisar (ex.: PC desligado).
   */
  async routeFindSuccessor(id, hops, logLookup) {
    const attempted = new Set();
    let candidate = this.closestPrecedingFinger(id);
    if (candidate.id === this.id) candidate = this.successor;

    while (candidate) {
      if (attempted.has(candidate.id)) break;
      attempted.add(candidate.id);

      if (logLookup) {
        chord(`Node ${this.id} encaminhando hash ${id} para Node ${candidate.id}`);
      }

      try {
        return await this.rpc(candidate, "/rpc/find-successor", {
          method: "POST",
          body: { id, hops: hops + 1 },
        });
      } catch (rpcError) {
        network(
          `Node ${candidate.id} inacessível ao rotear hash ${id}: ${rpcError.message}`,
        );

        // Se o candidato que falhou era (ou é) o sucessor, deixa a cura
        // compartilhada resolver — em vez de cada busca concorrente tentar
        // substituir por conta própria e disputar a mesma lista.
        const live = this.successor && candidate.id === this.successor.id
          ? await this.ensureLiveSuccessor()
          : this.successor;

        candidate = live && !attempted.has(live.id) ? live : null;
      }
    }

    throw new Error(`Nenhuma rota viva encontrada para localizar o sucessor do hash ${id}`);
  }

  async isReachable(node) {
    try {
      await this.rpc(node, "/rpc/successor");
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Garante que this.successor responde, corrigindo o anel se não. Só uma
   * execução roda por vez: se várias buscas descobrem a falha ao mesmo
   * tempo (ex.: os 5 lookups paralelos de refreshFingerTable), todas
   * aguardam e reaproveitam o mesmo resultado, em vez de cada uma podar a
   * successorList por conta própria — era essa disputa concorrente que
   * esvaziava a lista cedo demais e travava a rede em timeouts repetidos.
   */
  ensureLiveSuccessor() {
    if (!this._healingSuccessor) {
      this._healingSuccessor = this._healSuccessor().finally(() => {
        this._healingSuccessor = null;
      });
    }
    return this._healingSuccessor;
  }

  async _healSuccessor() {
    const startedFrom = this.successor ? this.successor.id : null;

    for (let attempts = 0; attempts < this.fingers.length + 3; attempts += 1) {
      if (!this.successor || this.successor.id === this.id) {
        return this.reference;
      }
      if (await this.isReachable(this.successor)) {
        if (this.successor.id !== startedFrom) this.notifyNewSuccessor(this.successor);
        return this.successor;
      }

      const deadId = this.successor.id;
      this.successorList = this.successorList.filter((node) => node.id !== deadId);
      const next = this.successorList.find((node) => node.id !== this.id) || null;

      if (!next) {
        this.successor = this.reference;
        this.predecessor = this.reference;
        error(
          `Node ${this.id} não encontrou substituto vivo para Node ${deadId}; assumindo o anel sozinho`,
        );
        return this.successor;
      }

      this.successor = next;
      chord(
        `Node ${this.id} substituiu sucessor inacessível (Node ${deadId}) por Node ${next.id}`,
      );
      // Continua o laço: confirma que `next` de fato responde antes de
      // devolvê-lo — se vários nós do mesmo computador caíram juntos, pode
      // ser preciso pular mais de um antes de achar um vivo de verdade.
    }
    throw new Error("Limite de tentativas excedido ao substituir sucessores inacessíveis");
  }

  /** Efeitos colaterais de assumir um novo sucessor — dispara em segundo plano. */
  notifyNewSuccessor(next) {
    this.rpc(next, "/rpc/predecessor", {
      method: "PUT",
      body: { node: this.reference },
    }).catch((notifyError) => {
      network(
        `Falha ao notificar Node ${next.id} sobre a troca de sucessor: ${notifyError.message}`,
      );
    });
    this.refreshFingerTable().catch(() => {});
    // Importante: NÃO recalcula successorList aqui a partir do novo
    // sucessor. Se ele também estiver morto, esse recálculo falharia cedo
    // e substituiria a lista boa (que ainda tem outros candidatos vivos
    // mais à frente) por uma lista truncada — foi isso que causava a rede
    // parecer travada num loop de timeouts. A lista já foi podada em
    // _healSuccessor; o próximo join/leave/refresh-fingers bem-sucedido é
    // quem a repõe com dados frescos.
    setImmediate(() => {
      this.rpc(next, "/rpc/refresh-fingers", {
        method: "POST",
        body: { originId: this.id, hops: 0 },
      }).catch(() => {});
      this.replicationManager.rebalanceAll().catch((rebalanceError) => {
        replication(
          `Falha ao rebalancear após troca de sucessor: ${rebalanceError.message}`,
        );
      });
    });
  }

  /** Recalcula os próximos sucessores vivos, usado como plano B de rota. */
  async refreshSuccessorList() {
    try {
      this.successorList = await this.replicationManager.getReplicaNodes(
        this.reference,
      );
    } catch (refreshError) {
      network(
        `Falha ao atualizar lista de sucessores do Node ${this.id}: ${refreshError.message}`,
      );
    }
  }

  closestPrecedingFinger(id) {
    for (let i = this.fingers.length - 1; i >= 0; i -= 1) {
      const candidate = this.fingers[i].node;
      if (
        candidate &&
        candidate.id !== this.id &&
        inInterval(candidate.id, this.id, id, false, false)
      ) {
        return candidate;
      }
    }
    return this.reference;
  }

  /** Insere bytes na rede e devolve a posição do hash e o nó responsável. */
  async put(fileName, content, { updateCatalog = true } = {}) {
    this.assertJoined();

    const name = validateFileName(fileName);
    section("[CHORD] NOVO UPLOAD");

    chord(`Solicitante : Node ${this.id}`);
    chord(`Arquivo     : ${name}`);
    const bytes = Buffer.isBuffer(content) ? content : Buffer.from(content);

    const hashId = hashKey(name);

    chord(`Hash        : ${hashId}`);

    // O Chord continua sendo responsável por descobrir o owner.
    const owner = await this.findSuccessor(hashId, 0, {
      log: true,
    });

    chord(`Owner       : Node ${owner.id}`);
    chord(`Tamanho     : ${bytes.length} bytes`);

    // A camada de replicação assume a partir daqui.
    const replication = await this.replicationManager.replicate(
      name,
      bytes,
      owner,
    );

    if (updateCatalog && name !== CATALOG_NAME) {
      await this.addToCatalog(name);
    }

    return {
      name,
      hashId,
      node: owner,
      size: bytes.length,
      replication,
    };
  }

  /** Busca os bytes de um arquivo a partir de qualquer nó da rede. */
  async get(fileName) {
    this.assertJoined();
    const name = validateFileName(fileName);
    const hashId = hashKey(name);

    // O catálogo é replicado em todos os nós (ver updateCatalogOnAllNodes).
    // Se este nó já tem uma cópia local, serve direto — evita depender do
    // roteamento pelo anel, que pode falhar se o "owner" calculado estiver
    // temporariamente inacessível mesmo com a rede ainda íntegra.
    if (name === CATALOG_NAME && (await this.hasLocal(name))) {
      const content = await this.readLocal(name);
      return {
        name,
        hashId,
        node: this.reference,
        size: content.length,
        content,
      };
    }

    const owner = await this.findSuccessor(hashId);
    let content;

    if (owner.id === this.id) {
      content = await this.readLocal(name);
    } else {
      const result = await this.rpc(
        owner,
        `/rpc/files?name=${encodeURIComponent(name)}`,
      );
      content = Buffer.from(result.content, "base64");
    }
    return { name, hashId, node: owner, size: content.length, content };
  }

  /** Lista os nomes de arquivos conhecidos pela rede (exclui o próprio catálogo). */
  async listCatalog() {
    try {
      const catalog = await this.get(CATALOG_NAME);

      return catalog.content.toString("utf8").split(/\r?\n/).filter(Boolean);
    } catch (error) {
      if (error.code === "ENOENT" || /não encontrado/i.test(error.message)) {
        return [];
      }
      throw error;
    }
  }

  async addToCatalog(fileName) {
    const names = await this.listCatalog();

    if (!names.includes(fileName)) {
      names.push(fileName);
    }

    names.sort((a, b) => a.localeCompare(b, "pt-BR"));

    const content = Buffer.from(`${names.join("\n")}\n`, "utf8");

    return this.updateCatalogOnAllNodes(content);
  }

  async storeLocal(fileName, content) {
    const name = validateFileName(fileName);
    await fs.mkdir(this.storageDirectory, { recursive: true });
    await fs.writeFile(path.join(this.storageDirectory, name), content);

    storage(`Node ${this.id} armazenou "${name}" (${content.length} bytes)`);
  }

  async readLocal(fileName) {
    const name = validateFileName(fileName);
    try {
      return await fs.readFile(path.join(this.storageDirectory, name));
    } catch (error) {
      if (error.code === "ENOENT") {
        const notFound = new Error(`Arquivo "${name}" não encontrado na rede`);
        notFound.code = "ENOENT";
        throw notFound;
      }
      throw error;
    }
  }

  async deleteLocal(fileName) {
    const name = validateFileName(fileName);
    try {
      await fs.unlink(path.join(this.storageDirectory, name));
      storage(`Node ${this.id} removeu cópia local de "${name}"`);
      return true;
    } catch (error) {
      if (error.code === "ENOENT") return false;
      throw error;
    }
  }

  async hasLocal(fileName) {
    const name = validateFileName(fileName);
    try {
      await fs.access(path.join(this.storageDirectory, name));
      return true;
    } catch {
      return false;
    }
  }

  assertJoined() {
    if (!this.joined) throw new Error("O nó ainda não entrou em uma rede");
  }

  /** true se `id` falhou recentemente e ainda está dentro do "período de quarentena". */
  isKnownDead(id) {
    const expiresAt = this.deadNodes.get(id);
    if (expiresAt === undefined) return false;
    if (Date.now() >= expiresAt) {
      this.deadNodes.delete(id);
      return false;
    }
    return true;
  }

  markDead(id) {
    this.deadNodes.set(id, Date.now() + this.deadNodeCooldownMs);
  }

  clearDead(id) {
    if (this.deadNodes.delete(id)) {
      network(`Node ${id} voltou a responder; removido da lista de inacessíveis`);
    }
  }

  async rpc(node, path, { method = "GET", body } = {}) {
    const target = normalizeReference(node);
    const isSelf = target.id === this.id;

    // Um nó marcado como morto recentemente falha na hora, sem esperar o
    // timeout de novo — é isso que evita a rede inteira ficar "presa"
    // tentando, uma operação de cada vez, alcançar quem já caiu.
    if (!isSelf && this.isKnownDead(target.id)) {
      const skip = new Error(
        `Node ${target.id} está marcado como inacessível (nova tentativa em instantes)`,
      );
      skip.code = "ETIMEDOUT";
      throw skip;
    }

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.requestTimeout);
    let response;
    try {
      response = await fetch(`http://${target.host}:${target.port}${path}`, {
        method,
        headers: body ? { "content-type": "application/json" } : undefined,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });
    } catch (networkError) {
      if (!isSelf) this.markDead(target.id);
      if (networkError.name === "AbortError") {
        const timeout = new Error(
          `Tempo limite ao acessar o nó ${target.id} em ${target.host}:${target.port}`,
        );
        timeout.code = "ETIMEDOUT";
        throw timeout;
      }
      throw networkError;
    } finally {
      clearTimeout(timer);
    }

    // Respondeu (mesmo que com erro HTTP de aplicação): o nó está vivo.
    if (!isSelf) this.clearDead(target.id);

    const data = await response.json();
    if (!response.ok)
      throw new Error(data.error || `Erro HTTP ${response.status}`);
    return data;
  }

  state() {
    return {
      node: this.reference,
      joined: this.joined,
      predecessor: this.predecessor,
      successor: this.successor,
      fingerTable: this.fingers,
    };
  }
}

function validateFileName(fileName) {
  if (typeof fileName !== "string" || !fileName.trim()) {
    throw new Error("O nome do arquivo é obrigatório");
  }
  const name = fileName.trim();
  if (
    name === "." ||
    name === ".." ||
    path.basename(name) !== name ||
    name.includes("/") ||
    name.includes("\\") ||
    name.includes("\0")
  ) {
    throw new Error("Nome de arquivo inválido");
  }
  return name;
}

function normalizeReference(node) {
  if (!node || typeof node !== "object")
    throw new Error("Referência de nó inválida");
  return {
    id: validateId(node.id),
    host: String(node.host || "127.0.0.1"),
    port: Number(node.port || 5000),
  };
}

module.exports = {
  ChordNode,
  normalizeReference,
  validateFileName,
  CATALOG_NAME,
};
