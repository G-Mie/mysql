import pymysql
import pymysql.cursors
import json
import os
import logging
import traceback
from queue import Queue
import threading
from contextlib import contextmanager
import datetime
import pandas as pd


# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('mysql_db')


class DatabaseError(Exception):
    """
    数据库操作相关的基础异常类
    """
    pass


class ConnectionError(DatabaseError):
    """
    数据库连接异常
    """
    pass


class QueryError(DatabaseError):
    """
    查询执行异常
    """
    pass


class UpdateError(DatabaseError):
    """
    更新操作异常
    """
    pass


class PoolExhaustedError(DatabaseError):
    """
    连接池资源耗尽异常
    """
    pass


class MySQLConnection:
    """
    MySQL数据库连接类，提供基本的数据库连接功能和灵活的配置选项
    """
    
    def __init__(self, host='localhost', user='root', password='', database='', 
                 port=3306, charset='utf8mb4', config=None, log_level=logging.INFO):
        """
        初始化数据库连接参数
        
        Args:
            host: 数据库主机地址
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            port: 数据库端口
            charset: 字符集
            config: 配置字典，可覆盖默认参数
            log_level: 日志级别
        """
        # 设置日志级别
        self.log_level = log_level
        logger.setLevel(log_level)
        
        # 初始化默认配置
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port,
            'charset': charset
        }
        
        # 如果提供了配置字典，使用它更新默认配置
        if config:
            self.config.update(config)
            
        self.connection = None
        logger.debug(f"MySQLConnection实例已创建，配置: {self._safe_config()}")
    
    def _safe_config(self):
        """
        返回安全的配置信息（不包含密码）
        
        Returns:
            dict: 安全的配置字典
        """
        safe_config = self.config.copy()
        if 'password' in safe_config:
            safe_config['password'] = '******'
        return safe_config
    



class MySQLConnectionPool:
    """
    MySQL数据库连接池，用于管理多个数据库连接以提高性能
    """
    
    def __init__(self, min_connections=5, max_connections=20, 
                 host='localhost', user='root', password='', database='', 
                 port=3306, charset='utf8mb4', config=None, log_level=logging.INFO,
                 connection_timeout=30):
        """
        初始化数据库连接池
        
        Args:
            min_connections: 最小连接数
            max_connections: 最大连接数
            host: 数据库主机地址
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名称
            port: 数据库端口
            charset: 字符集
            config: 配置字典
            log_level: 日志级别
            connection_timeout: 连接超时时间（秒）
        """
        # 设置日志级别
        self.log_level = log_level
        logger.setLevel(log_level)
        
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.current_connections = 0
        self.lock = threading.RLock()  # 可重入锁，用于线程安全
        self.connection_timeout = connection_timeout
        
        # 初始化默认配置
        self.config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port,
            'charset': charset
        }
        
        if config:
            self.config.update(config)
        
        # 创建连接队列
        self.connection_queue = Queue(maxsize=max_connections)
        
        logger.info(f"初始化连接池，最小连接数: {min_connections}，最大连接数: {max_connections}")
        logger.debug(f"连接池配置: {self._safe_config()}")
        
        # 初始化最小连接数
        self._initialize_pool()
    
    def _safe_config(self):
        """
        返回安全的配置信息（不包含密码）
        
        Returns:
            dict: 安全的配置字典
        """
        safe_config = self.config.copy()
        if 'password' in safe_config:
            safe_config['password'] = '******'
        return safe_config
    
    def _initialize_pool(self):
        """
        初始化连接池，创建最小连接数的连接
        """
        success_count = 0
        error_count = 0
        
        for i in range(self.min_connections):
            try:
                connection = self._create_connection()
                self.connection_queue.put(connection)
                self.current_connections += 1
                success_count += 1
                logger.debug(f"初始化连接 {i+1}/{self.min_connections} 成功")
            except Exception as e:
                error_count += 1
                logger.error(f"初始化连接 {i+1}/{self.min_connections} 失败: {str(e)}")
                logger.debug(f"错误详情: {traceback.format_exc()}")
        
        logger.info(f"连接池初始化完成，成功创建 {success_count} 个连接，失败 {error_count} 个连接")
    
    def _create_connection(self):
        """
        创建新的数据库连接
        
        Returns:
            pymysql.Connection: 数据库连接对象
            
        Raises:
            ConnectionError: 连接创建失败时抛出
        """
        try:
            start_time = datetime.datetime.now()
            connection = pymysql.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                port=self.config['port'],
                charset=self.config['charset'],
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=self.connection_timeout
            )
            end_time = datetime.datetime.now()
            logger.debug(f"创建连接耗时: {(end_time - start_time).total_seconds():.3f}秒")
            return connection
        except pymysql.err.OperationalError as e:
            logger.error(f"创建连接失败 - 操作错误: {str(e)}")
            raise ConnectionError(f"创建连接失败 - 操作错误: {str(e)}")
        except pymysql.err.ProgrammingError as e:
            logger.error(f"创建连接失败 - 配置错误: {str(e)}")
            raise ConnectionError(f"创建连接失败 - 配置错误: {str(e)}")
        except Exception as e:
            logger.error(f"创建连接失败: {str(e)}")
            raise ConnectionError(f"创建连接失败: {str(e)}")
    
    def get_connection(self):
        """
        从连接池获取连接
        
        Returns:
            pymysql.Connection: 数据库连接对象
            
        Raises:
            PoolExhaustedError: 连接池资源耗尽且无法获取连接时抛出
        """
        try:
            # 尝试从队列获取连接
            try:
                connection = self.connection_queue.get(block=False)
                # 检查连接是否有效
                if not self._is_connection_valid(connection):
                    logger.debug("获取到无效连接，重新创建")
                    try:
                        connection.close()
                    except:
                        pass
                    connection = self._create_connection()
                logger.debug(f"成功获取连接，当前活跃连接数: {self.current_connections}")
                return connection
            except Exception:
                with self.lock:
                    # 如果队列为空，且未达到最大连接数，创建新连接
                    if self.current_connections < self.max_connections:
                        logger.debug(f"连接池未满，创建新连接，当前连接数: {self.current_connections}/{self.max_connections}")
                        try:
                            connection = self._create_connection()
                            self.current_connections += 1
                            logger.debug(f"创建新连接成功，当前连接数: {self.current_connections}/{self.max_connections}")
                            return connection
                        except Exception as e:
                            logger.error(f"创建新连接失败: {str(e)}")
                            raise ConnectionError(f"创建新连接失败: {str(e)}")
                    else:
                        logger.warning(f"连接池已满，等待连接释放... 当前连接数: {self.current_connections}/{self.max_connections}")
                
                # 等待其他连接释放，设置超时
                try:
                    connection = self.connection_queue.get(block=True, timeout=self.connection_timeout)
                    # 检查连接是否有效
                    if not self._is_connection_valid(connection):
                        logger.debug("等待获取到的连接无效，重新创建")
                        try:
                            connection.close()
                        except:
                            pass
                        connection = self._create_connection()
                    logger.debug(f"等待后获取连接成功，当前活跃连接数: {self.current_connections}")
                    return connection
                except Exception as e:
                    logger.error(f"获取连接超时: {str(e)}")
                    raise PoolExhaustedError(f"连接池资源耗尽，无法获取连接，超时时间: {self.connection_timeout}秒")
        except Exception as e:
            logger.error(f"获取连接失败: {str(e)}")
            raise
    
    def _is_connection_valid(self, connection):
        """
        检查连接是否有效
        
        Args:
            connection: 数据库连接对象
            
        Returns:
            bool: 连接是否有效
        """
        try:
            connection.ping(reconnect=False)
            return True
        except Exception as e:
            logger.debug(f"连接检查失败: {str(e)}")
            return False
    
    def release_connection(self, connection):
        """
        释放连接回连接池
        
        Args:
            connection: 数据库连接对象
        """
        try:
            # 检查连接是否有效
            if self._is_connection_valid(connection):
                self.connection_queue.put(connection)
                logger.debug(f"连接已释放回连接池")
            else:
                logger.debug("释放无效连接")
                with self.lock:
                    self.current_connections -= 1
                try:
                    connection.close()
                except:
                    pass
        except Exception as e:
            logger.error(f"释放连接失败: {str(e)}")
            # 尝试关闭连接，避免资源泄露
            try:
                connection.close()
                with self.lock:
                    self.current_connections -= 1
            except:
                pass
    
    @contextmanager
    def connection(self):
        """
        上下文管理器，用于自动获取和释放连接
        
        Yields:
            pymysql.Connection: 数据库连接对象
        """
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.release_connection(conn)
    
    def execute_query(self, query, params=None):
        """
        使用连接池执行查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            list: 查询结果
            
        Raises:
            QueryError: 查询执行失败时抛出
        """
        try:
            logger.debug(f"执行查询: {query[:200]}... 参数: {params}")
            start_time = datetime.datetime.now()
            
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params or ())
                    result = cursor.fetchall()
                    
                    end_time = datetime.datetime.now()
                    logger.info(f"查询执行成功，返回{len(result)}条记录，耗时: {(end_time - start_time).total_seconds():.3f}秒")
                    return result
        except pymysql.err.ProgrammingError as e:
            logger.error(f"查询语法错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise QueryError(f"查询语法错误: {str(e)}")
        except Exception as e:
            logger.error(f"查询执行失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise QueryError(f"查询执行失败: {str(e)}")
    
    def execute_update(self, query, params=None):
        """
        使用连接池执行更新操作
        
        Args:
            query: SQL更新语句
            params: 更新参数
            
        Returns:
            int: 受影响的行数
            
        Raises:
            UpdateError: 更新执行失败时抛出
        """
        try:
            logger.debug(f"执行更新: {query[:200]}... 参数: {params}")
            start_time = datetime.datetime.now()
            
            with self.connection() as conn:
                with conn.cursor() as cursor:
                    try:
                        affected_rows = cursor.execute(query, params or ())
                        conn.commit()
                        
                        end_time = datetime.datetime.now()
                        logger.info(f"更新执行成功，影响{affected_rows}行，耗时: {(end_time - start_time).total_seconds():.3f}秒")
                        return affected_rows
                    except pymysql.err.IntegrityError as e:
                        conn.rollback()
                        logger.error(f"数据完整性错误: {str(e)}")
                        raise UpdateError(f"数据完整性错误: {str(e)}")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"更新执行失败: {str(e)}")
                        raise UpdateError(f"更新执行失败: {str(e)}")
        except UpdateError:
            raise
        except Exception as e:
            logger.error(f"更新操作异常: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise UpdateError(f"更新操作异常: {str(e)}")
    
    def close_all_connections(self):
        """
        关闭连接池中的所有连接
        """
        logger.info("开始关闭连接池中的所有连接")
        closed_count = 0
        error_count = 0
        
        with self.lock:
            while not self.connection_queue.empty():
                try:
                    connection = self.connection_queue.get(block=False)
                    try:
                        connection.close()
                        closed_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.warning(f"关闭连接时出错: {str(e)}")
                except Exception as e:
                    logger.error(f"获取待关闭连接时出错: {str(e)}")
            self.current_connections = 0
        
        logger.info(f"连接池关闭完成，成功关闭 {closed_count} 个连接，失败 {error_count} 个连接")
    
    @classmethod
    def from_config_file(cls, config_file, min_connections=5, max_connections=20):
        """
        从配置文件创建连接池
        
        Args:
            config_file: 配置文件路径
            min_connections: 最小连接数
            max_connections: 最大连接数
            
        Returns:
            MySQLConnectionPool: 连接池实例
            
        Raises:
            IOError: 配置文件读取失败时抛出
            ValueError: 配置文件格式错误时抛出
        """
        try:
            logger.info(f"从配置文件创建连接池: {config_file}")
            
            if not os.path.exists(config_file):
                logger.error(f"配置文件不存在: {config_file}")
                raise IOError(f"配置文件不存在: {config_file}")
            
            with open(config_file, 'r', encoding='utf-8') as f:
                try:
                    config = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"配置文件格式错误: {str(e)}")
                    raise ValueError(f"配置文件格式错误: {str(e)}")
            
            # 验证必要的配置项
            required_fields = ['host', 'user', 'password', 'database']
            for field in required_fields:
                if field not in config:
                    logger.error(f"配置文件缺少必要项: {field}")
                    raise ValueError(f"配置文件缺少必要项: {field}")
            
            logger.debug("配置文件加载成功")
            return cls(min_connections=min_connections, 
                      max_connections=max_connections,
                      config=config)
        except (IOError, ValueError):
            raise
        except Exception as e:
            logger.error(f"从配置文件加载失败: {str(e)}")
            raise Exception(f"从配置文件加载失败: {str(e)}")
    
    @classmethod
    def from_env(cls, min_connections=5, max_connections=20):
        """
        从环境变量创建连接池
        
        Args:
            min_connections: 最小连接数
            max_connections: 最大连接数
            
        Returns:
            MySQLConnectionPool: 连接池实例
        """
        logger.info("从环境变量创建连接池")
        config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', ''),
            'database': os.getenv('MYSQL_DATABASE', ''),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
        }
        logger.debug(f"环境变量配置: {cls._safe_config_static(config)}")
        return cls(min_connections=min_connections, 
                  max_connections=max_connections,
                  config=config)
    
    @staticmethod
    def _safe_config_static(config):
        """
        静态方法：返回安全的配置信息
        
        Args:
            config: 配置字典
            
        Returns:
            dict: 安全的配置字典
        """
        safe_config = config.copy()
        if 'password' in safe_config:
            safe_config['password'] = '******'
        return safe_config
        
    def _safe_config(self):
        """
        返回安全的配置信息（不包含密码）
        
        Returns:
            dict: 安全的配置字典
        """
        safe_config = self.config.copy()
        if 'password' in safe_config:
            safe_config['password'] = '******'
        return safe_config
    
    def connect(self):
        """
        建立数据库连接
        
        Returns:
            pymysql.Connection: 数据库连接对象
            
        Raises:
            ConnectionError: 连接失败时抛出
        """
        try:
            logger.info(f"正在连接到数据库 {self._safe_config()}")
            start_time = datetime.datetime.now()
            
            self.connection = pymysql.connect(
                host=self.config['host'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                port=self.config['port'],
                charset=self.config['charset'],
                cursorclass=pymysql.cursors.DictCursor  # 使用字典游标
            )
            
            end_time = datetime.datetime.now()
            logger.info(f"数据库连接成功，耗时: {(end_time - start_time).total_seconds():.3f}秒")
            return self.connection
        except pymysql.err.OperationalError as e:
            logger.error(f"数据库操作错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise ConnectionError(f"数据库操作错误: {str(e)}")
        except pymysql.err.InternalError as e:
            logger.error(f"数据库内部错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise ConnectionError(f"数据库内部错误: {str(e)}")
        except pymysql.err.ProgrammingError as e:
            logger.error(f"数据库编程错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise ConnectionError(f"数据库编程错误: {str(e)}")
        except Exception as e:
            logger.error(f"数据库连接失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise ConnectionError(f"数据库连接失败: {str(e)}")
    
    @classmethod
    def from_config_file(cls, config_file):
        """
        从JSON配置文件创建数据库连接实例
        
        Args:
            config_file: 配置文件路径
            
        Returns:
            MySQLConnection: 数据库连接实例
        """
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return cls(config=config)
        except Exception as e:
            raise Exception(f"从配置文件加载失败: {str(e)}")
    
    @classmethod
    def from_env(cls):
        """
        从环境变量创建数据库连接实例
        环境变量格式: MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT
        
        Returns:
            MySQLConnection: 数据库连接实例
        """
        config = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'root'),
            'password': os.getenv('MYSQL_PASSWORD', ''),
            'database': os.getenv('MYSQL_DATABASE', ''),
            'port': int(os.getenv('MYSQL_PORT', 3306)),
            'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4')
        }
        return cls(config=config)
    
    def update_config(self, config_dict):
        """
        更新配置参数
        
        Args:
            config_dict: 新的配置字典
        """
        if config_dict:
            self.config.update(config_dict)
            # 如果连接已建立，需要重新连接
            if self.connection:
                self.close()
                self.connect()
    
    def close(self):
        """
        关闭数据库连接
        """
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                logger.info("数据库连接已关闭")
            except Exception as e:
                logger.warning(f"关闭数据库连接时出错: {str(e)}")
                self.connection = None
    
    def execute_query(self, query, params=None):
        """
        执行SQL查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            list: 查询结果列表
            
        Raises:
            QueryError: 查询执行失败时抛出
        """
        if not self.connection:
            self.connect()
            
        try:
            logger.debug(f"执行查询: {query[:200]}... 参数: {params}")
            start_time = datetime.datetime.now()
            
            with self.connection.cursor() as cursor:
                cursor.execute(query, params or ())
                result = cursor.fetchall()
                
                end_time = datetime.datetime.now()
                logger.info(f"查询执行成功，返回{len(result)}条记录，耗时: {(end_time - start_time).total_seconds():.3f}秒")
                return result
        except pymysql.err.ProgrammingError as e:
            logger.error(f"查询语法错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise QueryError(f"查询语法错误: {str(e)}")
        except pymysql.err.InternalError as e:
            logger.error(f"查询内部错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise QueryError(f"查询内部错误: {str(e)}")
        except Exception as e:
            logger.error(f"查询执行失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise QueryError(f"查询执行失败: {str(e)}")
    
    def execute_update(self, query, params=None):
        """
        执行SQL更新语句（INSERT、UPDATE、DELETE等）
        
        Args:
            query: SQL更新语句
            params: 更新参数
            
        Returns:
            int: 受影响的行数
            
        Raises:
            UpdateError: 更新执行失败时抛出
        """
        if not self.connection:
            self.connect()
            
        try:
            logger.debug(f"执行更新: {query[:200]}... 参数: {params}")
            start_time = datetime.datetime.now()
            
            with self.connection.cursor() as cursor:
                affected_rows = cursor.execute(query, params or ())
                self.connection.commit()
                
                end_time = datetime.datetime.now()
                logger.info(f"更新执行成功，影响{affected_rows}行，耗时: {(end_time - start_time).total_seconds():.3f}秒")
                return affected_rows
        except pymysql.err.IntegrityError as e:
            self.connection.rollback()
            logger.error(f"数据完整性错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise UpdateError(f"数据完整性错误: {str(e)}")
        except pymysql.err.ProgrammingError as e:
            self.connection.rollback()
            logger.error(f"更新语法错误: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise UpdateError(f"更新语法错误: {str(e)}")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"更新执行失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise UpdateError(f"更新执行失败: {str(e)}")


# 简单示例
if __name__ == '__main__':
    # 创建数据库连接实例
    db = MySQLConnection(
        host='127.0.0.1',
        user='root',
        password='111111',
        database='day2025'
    )
    
    try:
        # 连接数据库
        db.connect()
        print("数据库连接成功")
        
        # 执行查询示例
        results = db.execute_query("SELECT 1 as test")
        print(f"查询结果: {results}")
        
    except Exception as e:
        print(f"错误: {str(e)}")
    finally:
        # 关闭连接
        db.close()
        print("数据库连接已关闭")