import ipaddress
import logging
import asyncio
from infrahub_sdk import InfrahubClient


async def run() -> None:
    client = InfrahubClient(address="http://localhost:8000")
    prefixes_batch = await client.create_batch()
    network_8 = ipaddress.IPv4Network("10.0.0.0/8")
    networks_16 = list(network_8.subnets(new_prefix=16))

    networks_24 = []
    for network in networks_16:
        tmp = list(network.subnets(new_prefix=24))[:4]
        networks_24.extend(tmp)

    networks = [network_8] + networks_16 + networks_24

    for network in networks:
        prefix = await client.create("IpamIPPrefix", prefix=f"{network}")
        await prefix.save(allow_upsert=True)
        # prefixes_batch.add(task=prefix.save, node=prefix, allow_upsert=True)
        # print(f"  Added prefix {network}")
    #
    # async for node, result in prefixes_batch.execute():
    #     print(f"prefix {node.prefix.value} was created in Infrahub succesfully")

result = asyncio.run(run())
