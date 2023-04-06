import { fork } from "child_process";
import { createReadStream } from "fs";
import { pipeline } from "stream/promises";
import { Writable } from "stream";
import csvtojson from "csvtojson";
import { setTimeout } from "timers";
const database = "./data/All_Pokemon.csv";
const PROCESS_COUNT = 10;

const replications = [];

const backgroundTaskFile = "./src/backgroundTask.js";

const processes = new Map();
let mainPipelineFinished = false;

for (let index = 0; index < PROCESS_COUNT; index++) {
  const child = fork(backgroundTaskFile, [database]);
  child.isFree = true;
  child.on("exit", () => {
    console.log(`process ${child.pid} exited`);
    processes.delete(child.pid);
  });
  child.on("error", () => {
    console.log(`process ${child.pid} has an error`, error);
    process.exit(1);
  });
  child.on("message", (msg) => {
    // workaround para multiprocessamento
    if (replications.includes(msg)) return;

    if (msg === "free") {
      // console.log("free");
      if (mainPipelineFinished) {
        child.kill();
      }
      child.isFree = true;
      return;
    }
    if (msg === "busy") {
      // console.log("busy");
      child.isFree = false;
      return;
    }

    console.log(`${msg} is replicated!`);
    replications.push(msg);
  });
  processes.set(child.pid, child);
}

function roundRobin(array, index = 0) {
  return function () {
    if (index >= array.length) index = 0;

    return array[index++];
  };
}

let execution = 1;

// Pool de conexões, ou load balancer
const getProcess = roundRobin([...processes.values()]);
console.log(`starting with ${processes.size} processes`);

const csvStream = createReadStream(database);

function asyncLoop(chunk, enc, cb) {
  let chosenProcess = {};
  chosenProcess.isFree = false;

  chosenProcess = getProcess();
  // console.log("IS FREE", chosenProcess.isFree);

  if (!chosenProcess.isFree) {
    setTimeout(() => {
      asyncLoop(chunk, enc, cb);
    }, 0);
    return;
  }

  chosenProcess.isFree = false;

  chosenProcess.send(JSON.parse(chunk));
  return cb();
}

await pipeline(
  csvStream,
  csvtojson(),
  Writable({
    write(chunk, enc, cb) {
      setTimeout(() => {
        asyncLoop(chunk, enc, cb);
      }, 0);

      // console.log("Execution", execution++);
      // let chosenProcess = {};
      // chosenProcess.isFree = false;
      // do {
      //   chosenProcess = getProcess();
      //   // console.log("IS FREE", chosenProcess.isFree);
      // } while (!chosenProcess.isFree);
      // chosenProcess.isFree = false;

      // chosenProcess.send(JSON.parse(chunk));
      // cb();
    },
  })
);
mainPipelineFinished = true;
processes.forEach((child) => {
  if (!child.killed) child.kill();
});
// console.log("Main FINISHED");
