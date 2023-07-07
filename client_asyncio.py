import json
import asyncio
from asyncua import Client

async def main():
    # 从 JSON 文件加载服务器信息和节点 ID
    with open('python/config.json') as file:
        config = json.load(file)

    server_info = config['server']
    nodes = config['nodes']

    # 连接到 OPC UA 服务器
    url = f"opc.tcp://{server_info['hostname']}:{server_info['port']}"
    async with Client(url=url) as client:
        # 读取每个节点的值
        for node in nodes:
            node_id = node['node_id']
            name = node['name']

            # 获取节点对象
            ua_node = await client.nodes.ua.Node.from_nodeid(node_id, client)

            # 读取节点的值
            value = await ua_node.read_value()

            # 打印节点名称和值
            print(f"{name}: {value}")

# 运行异步函数
asyncio.run(main())
