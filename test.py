import json
import csv
import time
from datetime import datetime
from opcua import Client

# 从 JSON 文件加载服务器信息和节点 ID
with open('python/config.json') as file:
    config = json.load(file)

server_info = config['server']
nodes = config['nodes']

# 连接到 OPC UA 服务器
client = Client(f"opc.tcp://{server_info['hostname']}:{server_info['port']}")
client.connect()

# 创建CSV文件
csv_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.csv'

# 提取节点名称列表
node_names = [node['name'] for node in nodes]

# 读取数据并缓存
data_cache = []
start_time = time.time()


while True:
    current_time = time.time()
    elapsed_time = current_time - start_time

    # 读取每个节点的值
    row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    for node in nodes:
        node_id = node['node_id']

        # 获取节点对象
        ua_node = client.get_node(node_id)

        # 读取节点的值
        value = ua_node.get_value()
        
        # 将浮点数限制为两位小数点
        value = round(value, 2)

        # 将节点值添加到数据行
        row.append(value)

    # 将数据行添加到数据缓存中
    data_cache.append(row)

    # 如果时间超过三分钟，将缓存的数据写入CSV文件
    if elapsed_time >= 10:
        with open(csv_filename, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            # 如果是新的csv文件，则写入列头数据 
            if csv_file.tell() == 0:
                writer.writerow(['TimeStamp'] + node_names)
            writer.writerows(data_cache)

        # 清空数据缓存
        data_cache = []
        start_time = time.time()
        csv_filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + '.csv'

    # 暂停500毫秒
    time.sleep(0.5)

# 断开连接
client.disconnect()


    









    

