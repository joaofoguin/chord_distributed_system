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
    this.predecessor = null;
    this.fingers = this.buildEmptyFingerTable();
    this.joined = false;

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

    await this.refreshFingerTable();

    // A entrada altera também as fingers dos nós que já estavam no anel.
    await this.rpc(this.successor, "/rpc/refresh-fingers", {
      method: "POST",
      body: { originId: this.id, hops: 0 },
    });
    return this.state();
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

    while (!visited.has(current.id)) {
      visited.add(current.id);
      nodes.push(current);

      if (current.id === this.id) {
        current = this.successor;
      } else {
        const result = await this.rpc(current, "/rpc/successor");
        current = normalizeReference(result.node);
      }

      if (!current) break;
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
    if (this.id === Number(originId)) return { ok: true };
    if (hops >= 32)
      throw new Error("Limite de nós excedido ao atualizar finger tables");

    await this.refreshFingerTable();

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

  async findSuccessor(rawId, hops = 0) {
    const id = validateId(rawId);

    if (hops === 0) {
      chord(`Localizando sucessor para hash ${id} a partir do Node ${this.id}`);
    }
    if (!this.joined || !this.successor)
      throw new Error("O nó ainda não entrou em uma rede");
    if (this.successor.id === this.id) return this.reference;
    if (id === this.id) return this.reference;

    if (inInterval(id, this.id, this.successor.id, false, true)) {
      chord(
        `Hash ${id} encontrado entre Node ${this.id} e Node ${this.successor.id} → sucessor Node ${this.successor.id}`,
      );

      return this.successor;
    }

    if (hops >= 32)
      throw new Error("Limite de saltos excedido ao procurar sucessor");
    let next = this.closestPrecedingFinger(id);

    chord(`Node ${this.id} encaminhando hash ${id} para Node ${next.id}`);
    // Uma finger table ainda desatualizada não deve interromper a busca:
    // caminhar pelo sucessor sempre encontra a posição correta no anel.
    if (next.id === this.id) next = this.successor;

    return this.rpc(next, "/rpc/find-successor", {
      method: "POST",
      body: { id, hops: hops + 1 },
    });
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
    const owner = await this.findSuccessor(hashId);

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

  async addToCatalog(fileName) {
    let names = [];

    try {
      const catalog = await this.get(CATALOG_NAME);

      names = catalog.content.toString("utf8").split(/\r?\n/).filter(Boolean);
    } catch (error) {
      if (error.code !== "ENOENT" && !/não encontrado/i.test(error.message)) {
        throw error;
      }
    }

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

  assertJoined() {
    if (!this.joined) throw new Error("O nó ainda não entrou em uma rede");
  }

  async rpc(node, path, { method = "GET", body } = {}) {
    const target = normalizeReference(node);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.requestTimeout);
    try {
      const response = await fetch(
        `http://${target.host}:${target.port}${path}`,
        {
          method,
          headers: body ? { "content-type": "application/json" } : undefined,
          body: body ? JSON.stringify(body) : undefined,
          signal: controller.signal,
        },
      );
      const data = await response.json();
      if (!response.ok)
        throw new Error(data.error || `Erro HTTP ${response.status}`);
      return data;
    } catch (error) {
      if (error.name === "AbortError") {
        const timeout = new Error(
          `Tempo limite ao acessar o nó ${target.id} em ${target.host}:${target.port}`,
        );
        timeout.code = "ETIMEDOUT";
        throw timeout;
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
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
