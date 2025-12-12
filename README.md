# MySQL数据库连接模块

本模块提供了一个功能完善的MySQL数据库连接工具，包括基本的数据库连接类和高性能的连接池实现，支持灵活的配置管理、异常处理和日志记录。

## 功能特性

- 基本的数据库连接功能，支持连接、查询、更新等操作
- 灵活的配置参数处理，支持直接参数、配置字典、配置文件和环境变量
- 高性能连接池实现，自动管理连接资源
- 完善的异常处理，定义了多种专用异常类
- 详细的日志记录，便于调试和监控
- 支持线程安全的并发操作

## 安装依赖

本模块依赖于`pymysql`库，使用前请先安装：

```bash
pip install pymysql
```

## 使用示例

### 1. 基本连接方式

使用`MySQLConnection`类进行基本的数据库连接：

```python
from mysql import MySQLConnection

# 创建数据库连接
conn = MySQLConnection(
    host='localhost',
    user='root',
    password='your_password',
    database='your_database',
    port=3306
)

# 连接数据库
conn.connect()

# 执行查询
result = conn.execute_query("SELECT * FROM users LIMIT 10")
print(f"查询结果: {result}")

# 执行更新
affected_rows = conn.execute_update(
    "INSERT INTO users (name, email) VALUES (%s, %s)",
    ('张三', 'zhangsan@example.com')
)
print(f"插入成功，影响行数: {affected_rows}")

# 关闭连接
conn.close()
```

### 2. 使用配置字典

```python
from mysql import MySQLConnection

# 定义配置字典
config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',
    'database': 'your_database'
}

# 使用配置字典创建连接
conn = MySQLConnection(config=config)
conn.connect()

# 执行操作...

conn.close()
```

### 3. 从配置文件加载

创建配置文件`db_config.json`：

```json
{
    "host": "localhost",
    "user": "root",
    "password": "your_password",
    "database": "your_database",
    "port": 3306,
    "charset": "utf8mb4"
}
```

然后从配置文件加载：

```python
from mysql import MySQLConnection

# 从配置文件创建连接
conn = MySQLConnection.from_config_file('db_config.json')
conn.connect()

# 执行操作...

conn.close()
```

### 4. 使用环境变量

设置环境变量：

```bash
# Linux/Mac
export MYSQL_HOST=localhost
export MYSQL_USER=root
export MYSQL_PASSWORD=your_password
export MYSQL_DATABASE=your_database

# Windows (命令提示符)
set MYSQL_HOST=localhost
set MYSQL_USER=root
set MYSQL_PASSWORD=your_password
set MYSQL_DATABASE=your_database

# Windows (PowerShell)
$env:MYSQL_HOST = "localhost"
$env:MYSQL_USER = "root"
$env:MYSQL_PASSWORD = "your_password"
$env:MYSQL_DATABASE = "your_database"
```

然后使用环境变量：

```python
from mysql import MySQLConnection

# 从环境变量创建连接
conn = MySQLConnection.from_env()
conn.connect()

# 执行操作...

conn.close()
```

### 5. 使用连接池

对于高并发场景，推荐使用连接池：

```python
from mysql import MySQLConnectionPool

# 创建连接池
pool = MySQLConnectionPool(
    min_connections=5,  # 最小连接数
    max_connections=20,  # 最大连接数
    host='localhost',
    user='root',
    password='your_password',
    database='your_database'
)

# 执行查询
result = pool.execute_query("SELECT * FROM users LIMIT 10")
print(f"查询结果: {result}")

# 执行更新
affected_rows = pool.execute_update(
    "INSERT INTO users (name, email) VALUES (%s, %s)",
    ('李四', 'lisi@example.com')
)
print(f"插入成功，影响行数: {affected_rows}")

# 手动获取和释放连接（高级用法）
conn = pool.get_connection()
try:
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) as count FROM users")
        result = cursor.fetchone()
        print(f"用户总数: {result['count']}")
finally:
    pool.release_connection(conn)

# 程序结束时关闭所有连接
pool.close_all_connections()
```

### 6. 连接池的配置方式

连接池也支持从配置文件和环境变量加载配置：

```python
from mysql import MySQLConnectionPool

# 从配置文件创建连接池
pool_from_config = MySQLConnectionPool.from_config_file('db_config.json')

# 从环境变量创建连接池
pool_from_env = MySQLConnectionPool.from_env()
```

### 7. 使用上下文管理器

连接池提供了上下文管理器支持，简化连接的获取和释放：

```python
from mysql import MySQLConnectionPool

pool = MySQLConnectionPool.from_env()

# 使用上下文管理器
with pool.connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users LIMIT 5")
        result = cursor.fetchall()
        print(result)
# 连接会自动释放回连接池
```

### 8. 异常处理

使用自定义异常类进行精确的异常处理：

```python
from mysql import MySQLConnectionPool, ConnectionError, QueryError, UpdateError, PoolExhaustedError

pool = MySQLConnectionPool.from_env()

try:
    # 执行操作
    result = pool.execute_query("SELECT * FROM nonexistent_table")
except ConnectionError as e:
    print(f"连接错误: {e}")
except QueryError as e:
    print(f"查询错误: {e}")
except PoolExhaustedError as e:
    print(f"连接池资源耗尽: {e}")
except Exception as e:
    print(f"其他错误: {e}")
```

## 异常类

模块定义了以下自定义异常类：

- `DatabaseError`：所有数据库异常的基类
- `ConnectionError`：连接相关的异常
- `QueryError`：查询执行异常
- `UpdateError`：更新操作异常
- `PoolExhaustedError`：连接池资源耗尽异常

## 日志配置

模块内置了日志系统，默认记录INFO级别以上的日志。您可以在初始化时调整日志级别：

```python
import logging
from mysql import MySQLConnection, MySQLConnectionPool

# 设置详细日志
conn = MySQLConnection(
    host='localhost',
    user='root',
    password='your_password',
    database='your_database',
    log_level=logging.DEBUG  # 详细日志
)

# 连接池也支持日志级别设置
pool = MySQLConnectionPool(
    min_connections=5,
    max_connections=20,
    host='localhost',
    user='root',
    password='your_password',
    database='your_database',
    log_level=logging.INFO  # 默认级别
)
```

## 最佳实践

1. **高并发场景使用连接池**：对于Web应用等并发场景，优先使用`MySQLConnectionPool`以提高性能

2. **配置外部化**：将数据库配置存储在配置文件或环境变量中，避免硬编码

3. **异常处理**：始终使用try-except捕获并处理可能的异常

4. **资源释放**：确保在使用完毕后关闭连接或释放回连接池

5. **参数化查询**：使用参数化查询避免SQL注入攻击

## 注意事项

- 确保`pymysql`库已正确安装
- 数据库配置信息（特别是密码）应妥善保管，避免泄露
- 对于长时间运行的应用，应定期检查连接状态，必要时重新连接
- 在高负载系统中，连接池的参数（最小/最大连接数）可能需要根据实际情况调整

## 版本信息

- 初始版本：1.0.0
- 支持Python 3.6及以上版本
- 依赖：pymysql
