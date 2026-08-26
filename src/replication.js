"use strict";

const { RING_SIZE, hashKey } = require("./ring");

const { section, replication, error } = require("./logger");

const DEFAULT_REPLICATION_FACTOR = 5;

class ReplicationManager {
  constructor(node, replicationFactor = DEFAULT_REPLICATION_FACTOR) {
    this.node = node;
    this.replicationFactor = Math.max(
      1,
      Math.min(Number(replicationFactor), RING_SIZE),
    );
  }

  /**
   * Retorna o owner e os sucessores seguintes que deverão
   * armazenar as cópias do arquivo.
   *
   * O próprio owner conta como uma das cópias.
   */
  async getReplicaNodes(owner) {
    const nodes = [owner];
    let current = owner;

    for (let i = 1; i < this.replicationFactor; i += 1) {
      let successor;

      if (current.id === this.node.id) {
        successor = this.node.successor;
      } else {
        try {
          const result = await this.node.rpc(current, "/rpc/successor");
          successor = result.node;
        } catch (rpcError) {
          // Um nó inacessível no meio da cadeia não pode travar o cálculo
          // das réplicas nem a lista de sucessores de reserva — encerra
          // aqui, com os nós já confirmados como vivos.
          break;
        }
      }

      if (!successor) break;

      // Evita duplicar o mesmo nó.
      if (nodes.some((node) => node.id === successor.id)) {
        break;
      }

      nodes.push(successor);
      current = successor;
    }

    return nodes;
  }

  /**
   * Distribui o arquivo pelo owner + sucessores.
   */
  async replicate(name, content, owner) {
    section("[REPLICATION] DISTRIBUIÇÃO DE ARQUIVO");

    replication(`Arquivo: ${name}`);
    replication(`Owner: Node ${owner.id}`);
    replication(`Fator de replicação: ${this.replicationFactor}`);

    const nodes = await this.getReplicaNodes(owner);

    replication(
      `Nós selecionados: ${nodes.map((node) => node.id).join(" → ")}`,
    );

    const results = [];

    for (let index = 0; index < nodes.length; index += 1) {
      const target = nodes[index];

      const role = index === 0 ? "OWNER" : "REPLICA";

      replication(`Node ${target.id} → ${role} | armazenando...`);

      try {
        if (target.id === this.node.id) {
          await this.node.storeLocal(name, content);
        } else {
          await this.node.rpc(target, "/rpc/files", {
            method: "PUT",
            body: {
              name,
              content: content.toString("base64"),
              role: index === 0 ? "OWNER" : "REPLICA",
              ownerId: owner.id,
            },
          });
        }

        replication(`Node ${target.id} → ${role} ✓`);

        results.push({
          id: target.id,
          host: target.host,
          port: target.port,
          role: index === 0 ? "OWNER" : "REPLICA",
          status: "ONLINE",
        });
      } catch (error) {
        replication(
          `Node ${target.id} → ${index === 0 ? "OWNER" : "REPLICA"} ✗`,
        );

        replication(`Motivo: ${error.message}`);
      }
    }

    const successfulCopies = results.filter(
      (result) => result.status === "ONLINE",
    );

    replication(
      `Cópias confirmadas: ${successfulCopies.length}/${this.replicationFactor}`,
    );

    if (successfulCopies.length === this.replicationFactor) {
      replication("Replicação concluída com sucesso.");
    } else {
      replication(
        `ATENÇÃO: apenas ${successfulCopies.length} cópias foram confirmadas.`,
      );
    }

    console.log("=".repeat(60));

    return {
      replicationFactor: this.replicationFactor,
      copies: results,
    };
  }

  /**
   * Recalcula, para um arquivo já existente, quais nós devem guardá-lo após
   * uma entrada ou saída no anel, e ajusta as cópias para que exatamente
   * `replicationFactor` nós (owner + sucessores) as mantenham — nem mais, nem menos.
   */
  async rebalanceFile(name) {
    const hashId = hashKey(name);
    const owner = await this.node.findSuccessor(hashId);
    const desired = await this.getReplicaNodes(owner);
    const desiredIds = new Set(desired.map((target) => target.id));

    const allNodes = await this.node.getAllNodes();

    const holders = [];
    for (const candidate of allNodes) {
      if (await this.remoteHasFile(candidate, name)) holders.push(candidate);
    }

    if (holders.length === 0) return { name, added: [], removed: [] };

    const source = holders.find((holder) => desiredIds.has(holder.id)) || holders[0];
    const content = await this.remoteReadFile(source, name);

    const added = [];
    for (const target of desired) {
      if (holders.some((holder) => holder.id === target.id)) continue;

      await this.remoteWriteFile(
        target,
        name,
        content,
        target.id === owner.id ? "OWNER" : "REPLICA",
        owner.id,
      );
      added.push(target.id);
    }

    const removed = [];
    for (const holder of holders) {
      if (desiredIds.has(holder.id)) continue;

      await this.remoteDeleteFile(holder, name);
      removed.push(holder.id);
    }

    if (added.length || removed.length) {
      replication(
        `Arquivo "${name}" rebalanceado | novas cópias: ${added.join(", ") || "-"} | removidas: ${removed.join(", ") || "-"}`,
      );
    }

    return { name, added, removed };
  }

  /**
   * Rebalanceia todos os arquivos conhecidos pelo catálogo. Chamado após
   * entrada/saída de nós — inclusive várias vezes seguidas quando mais de
   * um nó percebe uma falha ao mesmo tempo (ex.: um computador com vários
   * nós caiu de uma vez). Nesse caso, as chamadas extras aproveitam a
   * mesma execução em andamento em vez de disparar varreduras duplicadas
   * pela rede toda, que é o que fazia a reorganização parecer travada.
   */
  async rebalanceAll() {
    if (this._rebalanceInFlight) {
      replication("Rebalanceamento já em andamento; aproveitando a execução atual");
      return this._rebalanceInFlight;
    }

    this._rebalanceInFlight = this._runRebalanceAll().finally(() => {
      this._rebalanceInFlight = null;
    });
    return this._rebalanceInFlight;
  }

  async _runRebalanceAll() {
    const files = await this.node.listCatalog();
    if (files.length === 0) return [];

    section("[REPLICATION] REBALANCEAMENTO DO FATOR DE REPLICAÇÃO");
    replication(`Arquivos no catálogo: ${files.length}`);

    const results = [];
    for (const name of files) {
      try {
        results.push(await this.rebalanceFile(name));
      } catch (rebalanceError) {
        error(`Falha ao rebalancear "${name}": ${rebalanceError.message}`);
      }
    }

    console.log("=".repeat(60));
    return results;
  }

  async remoteHasFile(target, name) {
    if (target.id === this.node.id) return this.node.hasLocal(name);
    const result = await this.node.rpc(
      target,
      `/rpc/files/exists?name=${encodeURIComponent(name)}`,
    );
    return Boolean(result.exists);
  }

  async remoteReadFile(target, name) {
    if (target.id === this.node.id) return this.node.readLocal(name);
    const result = await this.node.rpc(
      target,
      `/rpc/files?name=${encodeURIComponent(name)}`,
    );
    return Buffer.from(result.content, "base64");
  }

  async remoteWriteFile(target, name, content, role, ownerId) {
    if (target.id === this.node.id) {
      return this.node.storeLocal(name, content);
    }
    return this.node.rpc(target, "/rpc/files", {
      method: "PUT",
      body: { name, content: content.toString("base64"), role, ownerId },
    });
  }

  async remoteDeleteFile(target, name) {
    if (target.id === this.node.id) return this.node.deleteLocal(name);
    return this.node.rpc(target, `/rpc/files?name=${encodeURIComponent(name)}`, {
      method: "DELETE",
    });
  }
}

module.exports = {
  ReplicationManager,
  DEFAULT_REPLICATION_FACTOR,
};
