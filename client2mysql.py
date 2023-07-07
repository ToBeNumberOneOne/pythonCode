import json
import time
from datetime import datetime
from opcua import Client
import mysql.connector
from mysql.connector import pooling

import logging



# 配置日志记录器
logging.basicConfig(filename='python/client2mysql.log', level=logging.INFO, format='%(asctime)s - [%(funcName)s-->line:%(lineno)d] - %(levelname)s:%(message)s')

# 从 JSON 文件加载服务器信息和节点 ID
with open('python/config.json') as file:
    config = json.load(file)

server_info = config['server']
nodes = config['nodes']

# 连接池配置
dbconfig = {
    "host": "172.30.160.1",
    "user": "root",
    "password": "Zpmc@3261",
    "database": "accs"
}

# 创建连接池
cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, pool_reset_session=True, **dbconfig)

def connect_to_opc_ua_server():
    # 连接到 OPC UA 服务器
    client = Client(f"opc.tcp://{server_info['hostname']}:{server_info['port']}")
    client.connect()
    logging.info(f"Connected to OPC UA server {server_info['hostname']}:{server_info['port']}")
    return client

def read_opc_ua_node_values(client):
    # 读取数据并插入到数据库
    values = []
    for node in nodes:
        node_id = node['node_id']

        # 获取节点对象
        ua_node = client.get_node(node_id)
        
        # 读取节点的值
        value = ua_node.get_value()
        value = round(value, 2)
        values.append(value)
    return values

def insert_data_to_mysql_database(data_cache):
    # 插入数据到 MySQL 数据库
    # 从连接池获取数据库连接
    cnx = cnxpool.get_connection()

    # 插入数据
    cursor = cnx.cursor()
    insert_query = f"INSERT INTO your_table_name (timestamp, "
    for node in nodes:
        node_name = node['name']
        insert_query += f"{node_name}, "
    insert_query = insert_query.rstrip(", ") + ") VALUES (%s, " + "%s, " * (len(nodes) - 1) + "%s)"
    cursor.executemany(insert_query, data_cache)
    cnx.commit()
    cursor.close()
    cnx.close()
    # 记录日志

    logging.info(f"Inserted {len(data_cache)} rows into MySQL database")
    

def clear_data_cache(data_cache):
    # 清空数据缓存
    data_cache.clear()

def main():
    logging.info("Starting client2mysql")
    data_cache = []
    while True:
        try:
            client = connect_to_opc_ua_server()
            while True:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                values = read_opc_ua_node_values(client)

                # 构建数据行
                data_cache.append((timestamp,) + tuple(values))

                # 如果数据缓存达到100条，则插入到数据库
                if len(data_cache) >= 10:
                    insert_data_to_mysql_database(data_cache)
                    clear_data_cache(data_cache)

                # 暂停500毫秒
                time.sleep(0.5)
        except Exception as e:
            logging.error(f"Failed to connect to OPC UA server: {e}")
            time.sleep(5)

    # 关闭连接
    client.disconnect()

if __name__ == '__main__':
    main()
