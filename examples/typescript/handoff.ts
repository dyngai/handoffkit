type Address = string;

type Context = {
  summary?: string;
  refs?: string[];
};

type Msg = {
  from: Address;
  to: Address;
  payload: string;
  ctx?: Context;
};

class Mailbox<T> {
  private queued: T[] = [];
  private waiters: Array<(value: T) => void> = [];

  send(value: T): void {
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter(value);
      return;
    }
    this.queued.push(value);
  }

  recv(): Promise<T> {
    if (this.queued.length > 0) {
      return Promise.resolve(this.queued.shift() as T);
    }
    return new Promise((resolve) => this.waiters.push(resolve));
  }
}

class Router {
  private mailboxes = new Map<Address, Mailbox<Msg>>();

  mailbox(address: Address): Mailbox<Msg> {
    let box = this.mailboxes.get(address);
    if (!box) {
      box = new Mailbox<Msg>();
      this.mailboxes.set(address, box);
    }
    return box;
  }

  send(msg: Msg): void {
    this.mailbox(msg.to).send(msg);
  }
}

async function planner(inbox: Mailbox<Msg>, router: Router): Promise<void> {
  const msg = await inbox.recv();
  const outline = [
    "1. State the problem",
    "2. Compare handoff vs shared state",
    "3. Name the tradeoffs",
  ].join("\n");

  router.send({
    from: "planner",
    to: "writer",
    payload: outline,
    ctx: { summary: `Task from ${msg.from}: ${msg.payload}` },
  });
}

async function writer(inbox: Mailbox<Msg>, router: Router): Promise<void> {
  const msg = await inbox.recv();
  router.send({
    from: "writer",
    to: "out",
    payload: [
      msg.ctx?.summary ?? "",
      "",
      `Writer received an owned outline:\n${msg.payload}`,
      "",
      "Final: message passing makes ownership explicit; shared state makes it implicit.",
    ].join("\n"),
  });
}

async function main(): Promise<void> {
  const router = new Router();
  void planner(router.mailbox("planner"), router);
  void writer(router.mailbox("writer"), router);

  router.send({
    from: "user",
    to: "planner",
    payload: "Explain why handoffs help agent coordination.",
  });

  const result = await router.mailbox("out").recv();
  console.log(result.payload);
}

void main();
