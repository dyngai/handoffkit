from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


Address = str


@dataclass(frozen=True)
class Context:
    summary: str = ""
    refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class Msg:
    sender: Address
    recipient: Address
    payload: str
    ctx: Context = field(default_factory=Context)


class Router:
    def __init__(self) -> None:
        self.mailboxes: dict[Address, asyncio.Queue[Msg]] = {}

    def mailbox(self, address: Address) -> asyncio.Queue[Msg]:
        return self.mailboxes.setdefault(address, asyncio.Queue(maxsize=1))

    async def send(self, msg: Msg) -> None:
        await self.mailbox(msg.recipient).put(msg)


async def planner(inbox: asyncio.Queue[Msg], router: Router) -> None:
    msg = await inbox.get()
    outline = "1. State the problem\n2. Compare handoff vs shared state\n3. Name the tradeoffs"
    await router.send(
        Msg(
            sender="planner",
            recipient="writer",
            payload=outline,
            ctx=Context(summary=f"Task from {msg.sender}: {msg.payload}"),
        )
    )


async def writer(inbox: asyncio.Queue[Msg], router: Router) -> None:
    msg = await inbox.get()
    answer = (
        f"{msg.ctx.summary}\n\n"
        f"Writer received an owned outline:\n{msg.payload}\n\n"
        "Final: message passing makes ownership explicit; shared state makes it implicit."
    )
    await router.send(Msg(sender="writer", recipient="out", payload=answer))


async def main() -> None:
    router = Router()
    async with asyncio.TaskGroup() as tg:
        tg.create_task(planner(router.mailbox("planner"), router))
        tg.create_task(writer(router.mailbox("writer"), router))

        await router.send(
            Msg(
                sender="user",
                recipient="planner",
                payload="Explain why handoffs help agent coordination.",
            )
        )
        result = await asyncio.wait_for(router.mailbox("out").get(), timeout=2)
        print(result.payload)


if __name__ == "__main__":
    asyncio.run(main())
