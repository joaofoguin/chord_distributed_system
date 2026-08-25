"use strict";

function timestamp() {
  return new Date().toLocaleTimeString("pt-BR", {
    hour12: false,
  });
}

function log(message = "") {
  console.log(`[${timestamp()}] ${message}`);
}

function section(title) {
  console.log("");
  console.log("=".repeat(60));
  console.log(title);
  console.log("=".repeat(60));
}

function chord(message) {
  log(`[CHORD] ${message}`);
}

function replication(message) {
  log(`[REPLICATION] ${message}`);
}

function storage(message) {
  log(`[STORAGE] ${message}`);
}

function network(message) {
  log(`[NETWORK] ${message}`);
}

function error(message) {
  log(`[ERROR] ${message}`);
}

module.exports = {
  log,
  section,
  chord,
  replication,
  storage,
  network,
  error,
};