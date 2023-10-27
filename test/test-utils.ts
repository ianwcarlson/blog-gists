import { spawn, exec } from "child_process";

export async function startRedis() {
  // await execShellCommand("docker stop redis || true");
  // await execShellCommand("docker rm redis || true");

  const ls = spawn("docker", [
    "run", "--rm", "-p", "6379:6379", "--name", "redis", "redis:6.2.7"
  ]);

  console.log("LS: " + JSON.stringify(ls));

  ls.stdout.on("data", (data) => {
    console.log(`${data}`);
  });

  ls.stderr.on("data", (data) => {
    console.error(`${data}`);
  });

  ls.on("error", (error) => {
    // if (!error.message.includes("spawn")) {
      console.log(`${error.message}`);
    // }
  });

  // ls.on("close", (code) => {
  //   console.log(`child process exited with code ${code}`);
  // });
}

export async function stopRedis() {
  await execShellCommand("docker stop redis || true");
  await execShellCommand("docker rm redis || true");
}

async function execShellCommand(command: string) {
  const response = await new Promise<void>(
    (resolve) => {
      exec(command, (error, stdout, stderr) => {
        if (error) {
          console.error(`${error.message}`);
          // reject(error);
        }
        if (stderr) {
          if (!stderr.includes("No such container: redis")) {
            console.error(`${stderr}`);
          }
          // resolve(error);
        }
        if (stdout) {
          console.log(`${stdout}`);
        }

        resolve();
      });
    }
  );
  return response;
}
