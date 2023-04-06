import { createReadStream } from "fs";
import { pipeline } from "stream/promises";
import { Writable, Transform } from "stream";
import csvtojson from "csvtojson";
import { setTimeout, clearTimeout } from "timers";
// import { createTracing } from "node:trace_events";

// const tracing = createTracing({
//   categories: ["v8", "node.async_hooks"],
// });

// tracing.enable();

const database = process.argv[2];

// let timeoutID = -1;
let count = 0;

function killProcess() {
  process.channel.unref();
}

async function onMessage(msg) {
  const firstTimeRan = [];
  const myTurn = count;
  count++;

  // console.log(`${myTurn} Started!`);

  // if (timeoutID === -1) {
  //   timeoutID = setTimeout(killProcess, 10000);
  // } else {
  //   clearTimeout(timeoutID);
  //   timeoutID = setTimeout(killProcess, 10000);
  // }

  process.send("busy");
  await pipeline(
    createReadStream(database),
    csvtojson(),
    Transform({
      transform(chunk, enc, cb) {
        const data = JSON.parse(chunk);

        if (data.Name !== msg.Name) return cb();

        if (firstTimeRan.includes(msg.Name)) {
          return cb(null, msg.Name);
        }

        firstTimeRan.push(msg.Name);

        cb();
      },
    }),
    Writable({
      write(chunk, enc, cb) {
        if (!chunk) return cb();

        process.send(chunk.toString());
        cb();
      },
    })
  );
  process.send("free");
  // console.log(`${myTurn} Finished!`);
}

process.on("message", onMessage);
process.on("SIGTERM", () => {
  process.exit(1);
});

// console.log(`I'm ready!! ${process.pid}`, database);

// para falar que o sub-processo pode morrer após inatividade
// await setTimeout(10000);
// process.channel.unref();
