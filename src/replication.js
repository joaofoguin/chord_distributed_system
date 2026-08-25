"use strict";

const { RING_SIZE } = require("./ring");

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
        const result = await this.node.rpc(current, "/rpc/successor");

        successor = result.node;
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
}

module.exports = {
  ReplicationManager,
  DEFAULT_REPLICATION_FACTOR,
};
