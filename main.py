#! /usr/bin/env python
import asyncio
import csv
import datetime
import os
from typing import Dict, Iterable

from aiohttp import ClientSession
from dotenv import load_dotenv
from returns.curry import curry, partial
from returns.future import future_safe
from returns.io import IO
from returns.pipeline import flow, is_successful
from returns.pointfree import bind_result, map_, bind
from returns.result import safe
from toolz import first
from toolz.curried import get, drop


async def main() -> None:
    load_dotenv()
    async with await starling_session() as session:
        entries = await flow(account_uuid(session),
                             map_(resource),
                             bind(lambda url: statement(session, url)),
                             map_(lambda x: x.split('\n')),
                             map_(drop(1)),
                             map_(partial(filter, lambda x: len(x) > 0)),
                             map_(split),
                             map_(partial(map, get([0, 1, 4, 5]))),
                             map_(partial(map, partial(map, lambda x: x.strip()))),
                             map_(partial(map, ','.join)),
                             map_('\n'.join))

        if is_successful(entries):
            IO.do(print(x) for x in entries)


def resource(account_uid: str):
    now = datetime.datetime.now()
    duration = datetime.timedelta(days=31)
    then = (now - duration).strftime('%Y-%m-%d')
    return f"/api/v2/accounts/{account_uid}/statement/downloadForDateRange?start={then}&end={now.strftime('%Y-%m-%d')}"


def split(xs: Iterable[str]) -> Iterable[Iterable[str]]:
    return [row for row in csv.reader(xs)]


def account_uuid(session: ClientSession):
    return flow(account(session),
                bind_result(safely_get('accounts')),
                bind_result(safely_first),
                bind_result(safely_get('accountUid')))


@curry
@safe
def safely_get(field: str, dictionary: Dict):
    return get(field, dictionary)


@safe
def safely_first(iterable):
    return first(iterable)


@future_safe
async def account(session: ClientSession) -> Dict:
    wtf = await session.get("/api/v2/accounts")
    return await wtf.json()


@future_safe
@curry
async def statement(session: ClientSession, resource_path: str):
    response = await session.get(resource_path, headers={'Accept': 'text/csv'})
    return await response.text()


async def starling_session() -> ClientSession:
    headers = {"Authorization": f"Bearer {os.environ['STARLING_ACCOUNT_ACCESS_TOKEN']}"}
    return ClientSession("https://api.starlingbank.com", headers=headers)


if __name__ == '__main__':
    asyncio.run(main())
