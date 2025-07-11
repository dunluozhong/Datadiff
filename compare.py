import importlib
import json
import os
import logging
import traceback
import concurrent.futures
import time
from datetime import datetime
from urllib.parse import urlparse
import re

from data_diff import connect_to_table, diff_tables

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseInspector:
    """数据库元数据检查器，用于表和键信息"""

    def __init__(self, db_url):
        self.db_url = db_url
        self.db_type = self._get_db_type()
        self.schema = None
        self.connection = self._create_connection()
        self.cursor = self.connection.cursor()

    def _get_db_type(self):
        """从URL中提取数据库类型"""
        return self.db_url.split("://")[0].lower()

    def _create_connection(self):
        """创建数据库连接并提取schema"""
        pattern = r'^(\w+)://([^:]+):([^@]+)@([^:/]+):(\d+)/([^/]+)/(.*)$'
        match = re.match(pattern, self.db_url)

        if match:
            db_type = match.group(1)
            username = match.group(2)
            password = match.group(3)
            host = match.group(4)
            port = int(match.group(5))
            database = match.group(6)
            schema_part = match.group(7)
            schema_parts = schema_part.split('/')
            schema = schema_parts[0] if schema_parts else None
        else:
            parsed = urlparse(self.db_url)
            username = parsed.username
            password = parsed.password
            host = parsed.hostname
            port = parsed.port
            path_parts = parsed.path.strip('/').split('/')
            database = path_parts[0] if path_parts else None
            schema = path_parts[1] if len(path_parts) > 1 else None

        if self.db_type == 'postgresql':
            self.schema = schema if schema else 'public'
        elif self.db_type == 'mssql':
            self.schema = schema if schema else 'dbo'
        elif self.db_type == 'oracle':
            self.schema = (schema or username).upper() if username else None

        drivers = {
            'postgresql': ('psycopg2', None),
            'mysql': ('mysql.connector', 'mysql-connector-python'),
            'oracle': ('oracledb', 'oracledb'),
            'mssql': ('pyodbc', 'pyodbc')
        }

        driver_module, package_name = drivers.get(self.db_type, (None, None))
        if not driver_module:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")

        try:
            module = importlib.import_module(driver_module)
            if self.db_type == 'oracle':
                try:
                    module.init_oracle_client()
                    logger.info("Oracle thick模式初始化成功")
                except Exception as e:
                    raise ImportError(f"Oracle客户端初始化失败: {str(e)}")
        except ImportError:
            if package_name:
                raise ImportError(f"请安装数据库驱动: pip install {package_name}")
            raise

        if self.db_type == 'postgresql':
            return module.connect(
                host=host,
                port=port or 5432,
                dbname=database,
                user=username,
                password=password
            )
        elif self.db_type == 'mysql':
            return module.connect(
                host=host,
                port=port or 3306,
                database=database,
                user=username,
                password=password
            )
        elif self.db_type == 'oracle':
            if not host:
                return module.connect(user=username, password=password, dsn=database)
            else:
                return module.connect(
                    user=username,
                    password=password,
                    host=host,
                    port=port or 1521,
                    service_name=database
                )
        elif self.db_type == 'mssql':
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={host},{port or 1433};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password}"
            )
            return module.connect(conn_str)

    def get_all_tables(self):
        """获取所有表名（保留原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                self.cursor.execute(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.schema}'
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
            elif self.db_type == 'mysql':
                self.cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE()
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
            elif self.db_type == 'mssql':
                self.cursor.execute(f"""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = '{self.schema}'
                    ORDER BY TABLE_NAME
                """)
            elif self.db_type == 'oracle':
                self.cursor.execute(f"""
                    SELECT table_name 
                    FROM dba_tables 
                    WHERE owner = '{self.schema}'
                    ORDER BY table_name
                """)
            else:
                raise ValueError(f"不支持的数据库类型: {self.db_type}")

            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取表列表失败: {str(e)}")
            return []

    def get_table_columns(self, table_name):
        """获取表的所有列名（保留原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                self.cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    AND table_schema = '{self.schema}'
                """)
            elif self.db_type == 'mysql':
                self.cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                    AND table_schema = DATABASE()
                """)
            elif self.db_type == 'mssql':
                self.cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = '{table_name}'
                    AND TABLE_SCHEMA = '{self.schema}'
                """)
            elif self.db_type == 'oracle':
                self.cursor.execute(f"""
                    SELECT column_name 
                    FROM all_tab_cols 
                    WHERE table_name = UPPER('{table_name}')
                    AND owner = '{self.schema}'
                """)
            else:
                return []

            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取表 {table_name} 的列失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return []

    def get_table_keys(self, table_name):
        """获取表的主键或唯一键（保留原始大小写）"""
        primary_keys = self._get_primary_keys(table_name)
        if primary_keys:
            return primary_keys, "主键"

        unique_keys = self._get_unique_keys(table_name)
        if unique_keys:
            return unique_keys, "唯一键"

        return "", None

    def _get_primary_keys(self, table_name):
        """获取主键列（保留原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                self.cursor.execute(f"""
                    SELECT kcu.column_name 
                    FROM information_schema.table_constraints tc 
                    JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name 
                    WHERE tc.table_name = '{table_name}' 
                    AND tc.table_schema = '{self.schema}'
                    AND tc.constraint_type = 'PRIMARY KEY'
                    ORDER BY kcu.ordinal_position
                """)
            elif self.db_type == 'mysql':
                self.cursor.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
                rows = self.cursor.fetchall()
                if rows:
                    column_name_idx = self._get_column_index(self.cursor.description, 'Column_name')
                    seq_idx = self._get_column_index(self.cursor.description, 'Seq_in_index')
                    rows.sort(key=lambda row: row[seq_idx])
                    primary_keys = [row[column_name_idx] for row in rows]
                    return ",".join(primary_keys)
                else:
                    return ""
            elif self.db_type == 'mssql':
                self.cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = '{table_name}'
                    AND TABLE_SCHEMA = '{self.schema}'
                    AND CONSTRAINT_NAME LIKE 'PK_%'
                    ORDER BY ORDINAL_POSITION
                """)
            elif self.db_type == 'oracle':
                self.cursor.execute(f"""
                    SELECT cols.column_name 
                    FROM all_constraints cons, all_cons_columns cols
                    WHERE cons.constraint_type = 'P'
                    AND cons.constraint_name = cols.constraint_name
                    AND cons.owner = '{self.schema}'
                    AND cols.owner = '{self.schema}'
                    AND cols.table_name = UPPER('{table_name}')
                    ORDER BY cols.position
                """)
            else:
                return ""

            results = [row[0] for row in self.cursor.fetchall()]
            return ",".join(results) if results else ""
        except Exception as e:
            logger.error(f"获取表 {table_name} 的主键失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return ""

    def _get_unique_keys(self, table_name):
        """获取唯一键列（保留原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                self.cursor.execute(f"""
                    SELECT kcu.column_name 
                    FROM information_schema.table_constraints tc 
                    JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name 
                    WHERE tc.table_name = '{table_name}' 
                    AND tc.table_schema = '{self.schema}'
                    AND tc.constraint_type = 'UNIQUE'
                    ORDER BY kcu.ordinal_position
                """)
            elif self.db_type == 'mysql':
                self.cursor.execute(f"SHOW INDEX FROM `{table_name}` WHERE Non_unique = 0 AND Key_name != 'PRIMARY'")
                rows = self.cursor.fetchall()
                if rows:
                    column_name_idx = self._get_column_index(self.cursor.description, 'Column_name')
                    key_name_idx = self._get_column_index(self.cursor.description, 'Key_name')
                    seq_idx = self._get_column_index(self.cursor.description, 'Seq_in_index')
                    rows.sort(key=lambda row: (row[key_name_idx], row[seq_idx]))

                    unique_constraints = {}
                    for row in rows:
                        key_name = row[key_name_idx]
                        col_name = row[column_name_idx]
                        if key_name not in unique_constraints:
                            unique_constraints[key_name] = []
                        unique_constraints[key_name].append(col_name)

                    if unique_constraints:
                        first_key = sorted(unique_constraints.keys())[0]
                        return ",".join(unique_constraints[first_key])
                return ""
            elif self.db_type == 'mssql':
                self.cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = '{table_name}'
                    AND TABLE_SCHEMA = '{self.schema}'
                    AND CONSTRAINT_NAME LIKE 'UQ_%'
                    ORDER BY ORDINAL_POSITION
                """)
            elif self.db_type == 'oracle':
                self.cursor.execute(f"""
                    SELECT cols.column_name 
                    FROM all_constraints cons, all_cons_columns cols
                    WHERE cons.constraint_type = 'U'
                    AND cons.constraint_name = cols.constraint_name
                    AND cons.owner = '{self.schema}'
                    AND cols.owner = '{self.schema}'
                    AND cols.table_name = UPPER('{table_name}')
                    ORDER BY cols.position
                """)
            else:
                return ""

            results = [row[0] for row in self.cursor.fetchall()]
            return ",".join(results) if results else ""
        except Exception as e:
            logger.error(f"获取表 {table_name} 的唯一键失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return ""

    def _get_column_index(self, description, column_name):
        """获取结果集中列的索引"""
        for idx, col in enumerate(description):
            if col[0].lower() == column_name.lower():
                return idx
        if column_name.lower() == 'column_name':
            return 4
        elif column_name.lower() == 'key_name':
            return 2
        elif column_name.lower() == 'seq_in_index':
            return 3
        return 0

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


class SingleTableDiffReporter:
    """单表差异报告器，提供增强的键信息"""

    def __init__(self, db1_url, db2_url, source_table_name, target_table_name,
                 source_key_columns, target_key_columns,
                 source_extra_columns=None, target_extra_columns=None,
                 source_where=None, target_where=None):
        self.db1_url = db1_url
        self.db2_url = db2_url
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.source_key_columns = source_key_columns
        self.target_key_columns = target_key_columns
        self.source_extra_columns = source_extra_columns or ""
        self.target_extra_columns = target_extra_columns or ""
        self.source_where = source_where
        self.target_where = target_where

        self.source = None
        self.target = None
        self.missing_rows = 0
        self.added_rows = 0
        self.error = None

    def execute_comparison(self):
        """执行表对比并返回结果"""
        try:
            src_inspector = DatabaseInspector(self.db1_url)
            tgt_inspector = DatabaseInspector(self.db2_url)

            result = {
                "源库表名": self.source_table_name,
                "目标库表名": self.target_table_name,
                "状态": "",
                "键情况": {},
                "差异详情": "",
                "缺失行": 0,
                "新增行": 0,
                "总差异数": 0,
                "错误详情": ""
            }

            # 检查表是否存在
            src_tables = src_inspector.get_all_tables()
            tgt_tables = tgt_inspector.get_all_tables()

            if self.source_table_name not in src_tables:
                err_msg = f"源表 '{self.source_table_name}' 不存在"
                logger.error(err_msg)
                result.update({
                    "状态": "[错误] 源表不存在",
                    "差异详情": err_msg,
                    "错误详情": err_msg
                })
                return result

            if self.target_table_name not in tgt_tables:
                err_msg = f"目标表 '{self.target_table_name}' 不存在"
                logger.error(err_msg)
                result.update({
                    "状态": "[错误] 目标表不存在",
                    "差异详情": err_msg,
                    "错误详情": err_msg
                })
                return result

            # 获取表的键和列信息
            src_keys, src_key_type = src_inspector.get_table_keys(self.source_table_name)
            tgt_keys, tgt_key_type = tgt_inspector.get_table_keys(self.target_table_name)
            src_columns = src_inspector.get_table_columns(self.source_table_name)
            tgt_columns = tgt_inspector.get_table_columns(self.target_table_name)

            # 使用提供的键或自动检测的键
            actual_source_keys = self.source_key_columns or src_keys
            actual_target_keys = self.target_key_columns or tgt_keys

            # 处理键列不匹配
            if not actual_source_keys or not actual_target_keys:
                logger.warning("键列不完整，尝试列名匹配")
                key_error = ""

                if src_keys and not actual_target_keys:
                    source_key_list = [k.strip() for k in src_keys.split(',') if k.strip()]
                    matched_tgt_keys = self.find_matching_columns(tgt_columns, source_key_list)
                    if matched_tgt_keys:
                        actual_target_keys = ",".join(matched_tgt_keys)
                    else:
                        key_error += " - 目标表无法匹配源键列"

                if tgt_keys and not actual_source_keys:
                    target_key_list = [k.strip() for k in tgt_keys.split(',') if k.strip()]
                    matched_src_keys = self.find_matching_columns(src_columns, target_key_list)
                    if matched_src_keys:
                        actual_source_keys = ",".join(matched_src_keys)
                    else:
                        key_error += " - 源表无法匹配目标键列"

                if not actual_source_keys or not actual_target_keys:
                    if not key_error:
                        key_error = "无法确定有效的键列"
                    logger.error(key_error)
                    result.update({
                        "状态": "[错误] 无有效键列",
                        "差异详情": key_error,
                        "错误详情": key_error
                    })
                    return result

            # 更新实例键列
            self.source_key_columns = actual_source_keys
            self.target_key_columns = actual_target_keys

            # 连接表
            if not self._connect_to_tables():
                result.update({
                    "状态": "[错误] 连接失败",
                    "差异详情": self.error or "未知错误",
                    "错误详情": self.error
                })
                return result

            # 执行差异对比
            if not self._generate_diff():
                result.update({
                    "状态": "[错误] 比对失败",
                    "差异详情": self.error or "未知错误",
                    "错误详情": self.error
                })
                return result

            # 准备键信息字典
            key_info = {
                "key_columns": f"键列: 源->{self.source_key_columns}, 目标-> {self.target_key_columns}",
                "extra_columns": f"额外列: 源->{self.source_extra_columns}, 目标->{self.target_extra_columns}",
                "where": f"过滤条件: 源->{self.source_where or '无'}, 目标->{self.target_where or '无'}"
            }

            # 准备状态和结果
            if self.missing_rows == 0 and self.added_rows == 0:
                status = "[一致]"
                diff_detail = "无差异"
            else:
                status = "[不一致]"
                diff_detail = f"缺失行: {self.missing_rows}, 新增行: {self.added_rows}"

            result.update({
                "状态": status,
                "键情况": key_info,
                "差异详情": diff_detail,
                "缺失行": self.missing_rows,
                "新增行": self.added_rows,
                "总差异数": self.missing_rows + self.added_rows
            })

            return result
        except Exception as e:
            logger.error(f"表对比异常: {str(e)}")
            logger.debug(traceback.format_exc())
            return {
                "源库表名": self.source_table_name,
                "目标库表名": self.target_table_name,
                "状态": "[错误] 比对异常",
                "键情况": {
                    "key_columns": f"源键列: {self.source_key_columns}, 目标键列: {self.target_key_columns}",
                    "extra_columns": f"额外列: 源->{self.source_extra_columns}, 目标->{self.target_extra_columns}",
                    "where": f"过滤条件: 源->{self.source_where}, 目标->{self.target_where}"
                },
                "差异详情": f"比对异常: {str(e)}",
                "缺失行": 0,
                "新增行": 0,
                "总差异数": 0,
                "错误详情": traceback.format_exc()
            }
        finally:
            src_inspector.close()
            tgt_inspector.close()

    def _connect_to_tables(self):
        """连接到源表和目标表"""
        try:
            logger.info(f"正在连接表: {self.source_table_name} -> {self.target_table_name}")

            def parse_and_clean_url(url, db_type):
                pattern = r'^(\w+)://([^:]+):([^@]+)@([^:/]+):(\d+)/([^/]+)/(.*)$'
                match = re.match(pattern, url)

                if match:
                    db_type = match.group(1)
                    username = match.group(2)
                    password = match.group(3)
                    host = match.group(4)
                    port = int(match.group(5))
                    database = match.group(6)
                    schema = match.group(7)

                    if db_type == 'oracle' and schema:
                        schema = schema.upper()

                    clean_url = f"{db_type}://{username}:{password}@{host}:{port}/{database}"
                    return clean_url, schema
                else:
                    parsed = urlparse(url)
                    username = parsed.username
                    password = parsed.password
                    host = parsed.hostname
                    port = parsed.port
                    path_parts = parsed.path.strip('/').split('/')
                    database = path_parts[0] if path_parts else None
                    schema = path_parts[1] if len(path_parts) > 1 else None

                    if db_type == 'oracle' and schema:
                        schema = schema.upper()

                    clean_path = f"/{database}" if database else ""
                    clean_url = parsed._replace(path=clean_path).geturl()
                    return clean_url, schema

            src_db_type = self.db1_url.split("://")[0].lower()
            tgt_db_type = self.db2_url.split("://")[0].lower()

            src_clean_url, src_schema = parse_and_clean_url(self.db1_url, src_db_type)
            tgt_clean_url, tgt_schema = parse_and_clean_url(self.db2_url, tgt_db_type)

            def format_table_name(table_name, db_type, schema):
                if db_type in ['postgresql', 'mssql', 'oracle'] and schema:
                    return f"{schema}.{table_name}"
                return table_name

            source_table = format_table_name(
                self.source_table_name, src_db_type, src_schema
            )
            target_table = format_table_name(
                self.target_table_name, tgt_db_type, tgt_schema
            )

            # SQL Server的特殊处理
            if src_db_type == 'mssql' and src_schema:
                if ";" in src_clean_url:
                    src_clean_url += f"/{src_schema}"
                else:
                    src_clean_url += f"/{src_schema}"

            if tgt_db_type == 'mssql' and tgt_schema:
                if ";" in tgt_clean_url:
                    tgt_clean_url += f"/{tgt_schema}"
                else:
                    tgt_clean_url += f"/{tgt_schema}"

            # 拆分键列
            source_keys = self.source_key_columns.split(',') if self.source_key_columns else []
            target_keys = self.target_key_columns.split(',') if self.target_key_columns else []
            source_keys = [k.strip() for k in source_keys if k.strip()]
            target_keys = [k.strip() for k in target_keys if k.strip()]

            # 拆分额外列
            source_extra = self.source_extra_columns.split(',') if self.source_extra_columns else []
            target_extra = self.target_extra_columns.split(',') if self.target_extra_columns else []
            source_extra = [k.strip() for k in source_extra if k.strip()]
            target_extra = [k.strip() for k in target_extra if k.strip()]

            # 连接表
            self.source = connect_to_table(
                src_clean_url,
                table_name=source_table,
                key_columns=source_keys,
                extra_columns=source_extra,
                where=self.source_where
            )

            self.target = connect_to_table(
                tgt_clean_url,
                table_name=target_table,
                key_columns=target_keys,
                extra_columns=target_extra,
                where=self.target_where
            )

            logger.info("表连接成功")
            return True
        except Exception as e:
            self.error = f"表连接失败: {str(e)}"
            logger.error(self.error)
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return False

    def _generate_diff(self):
        """生成数据差异报告"""
        try:
            logger.info(f"正在比较表: {self.source_table_name} -> {self.target_table_name}")

            # 执行差异对比
            diff = diff_tables(
                self.source,
                self.target,
                threaded=True
            )

            # 统计差异
            for diff_type, _ in diff:
                if diff_type == '-':
                    self.missing_rows += 1
                elif diff_type == '+':
                    self.added_rows += 1

            logger.info(f"表比较完成: {self.source_table_name} -> {self.target_table_name}")
            return True
        except Exception as e:
            self.error = f"比较错误: {str(e)}"
            logger.error(f"{self.source_table_name} -> {self.target_table_name} 比较失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return False

    @staticmethod
    def find_matching_columns(columns, key_list):
        """在表中查找匹配的列（不区分大小写）"""
        matched_keys = []
        for key in key_list:
            matched = False
            for col in columns:
                if col.lower() == key.lower():
                    matched_keys.append(col)
                    matched = True
                    break
            if not matched:
                logger.warning(f"键列 '{key}' 在表中无匹配项")
                return None
        return matched_keys


class TableDiffReporter:
    """数据库差异报告器，支持全库、单表和多表比较"""

    def __init__(self, db1_url, db2_url,
                 source_table_name=None, target_table_name=None,
                 source_key_columns=None, target_key_columns=None,
                 source_extra_columns=None, target_extra_columns=None,
                 source_where=None, target_where=None,
                 table_pairs=None, key_mappings=None, extra_columns_map=None, where_map=None,
                 realtime_report_path=None):
        self.db1_url = db1_url
        self.db2_url = db2_url
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.source_key_columns = source_key_columns
        self.target_key_columns = target_key_columns
        self.source_extra_columns = source_extra_columns or ""
        self.target_extra_columns = target_extra_columns or ""
        self.source_where = source_where
        self.target_where = target_where
        self.table_pairs = table_pairs or []
        self.key_mappings = key_mappings or {}
        self.extra_columns_map = extra_columns_map or {}
        self.where_map = where_map or {}
        self.realtime_report_path = realtime_report_path

        self.all_results = []
        self.start_time = time.time()
        self.report_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.total_tables = 0
        self.completed_tables = 0

        self.mode = self._detect_mode()
        logger.info(f"检测到比较模式: {self.mode}")

    def _detect_mode(self):
        """自动检测比较模式"""
        if self.table_pairs:
            logger.info(f"多表比较模式 (显式表对): {len(self.table_pairs)} 张表")
            return "multi_table"

        if (isinstance(self.source_table_name, list) and
                isinstance(self.target_table_name, list)):
            if len(self.source_table_name) != len(self.target_table_name):
                raise ValueError("源表和目标表数量不匹配")

            self.table_pairs = list(zip(self.source_table_name, self.target_table_name))

            if (isinstance(self.source_key_columns, list) and
                    isinstance(self.target_key_columns, list)):
                if (len(self.source_table_name) != len(self.source_key_columns) or
                        len(self.source_table_name) != len(self.target_key_columns)):
                    raise ValueError("表名列表和键列列表长度不匹配")

                self.key_mappings = {
                    src_table: (src_keys, tgt_keys)
                    for src_table, src_keys, tgt_keys in zip(
                        self.source_table_name, self.source_key_columns, self.target_key_columns
                    )
                }

            if (isinstance(self.source_extra_columns, list) and
                    isinstance(self.target_extra_columns, list)):
                self.extra_columns_map = {
                    src_table: (src_extra, tgt_extra)
                    for src_table, src_extra, tgt_extra in zip(
                        self.source_table_name, self.source_extra_columns, self.target_extra_columns
                    )
                }

            if (isinstance(self.source_where, list) and
                    isinstance(self.target_where, list)):
                self.where_map = {
                    src_table: (src_where, tgt_where)
                    for src_table, src_where, tgt_where in zip(
                        self.source_table_name, self.source_where, self.target_where
                    )
                }

            logger.info(f"多表比较模式 (列表形式): {len(self.table_pairs)} 张表")
            return "multi_table"

        if (isinstance(self.source_table_name, str) and
                isinstance(self.target_table_name, str)):
            logger.info(f"单表比较模式: {self.source_table_name}->{self.target_table_name}")
            return "single_table"

        logger.info("检测到全库比较模式")
        return "full_db"

    def run_report(self, max_threads=5):
        """执行数据库比较并生成报告"""
        start_time = time.time()
        self._init_realtime_report()

        if self.mode == "full_db":
            logger.info(f"开始全库比较: {self.db1_url} -> {self.db2_url}")
            self._run_full_database_comparison(max_threads, start_time)
        elif self.mode == "single_table":
            logger.info(f"开始单表比较: {self.source_table_name} -> {self.target_table_name}")
            self._run_single_table_comparison()
        elif self.mode == "multi_table":
            logger.info(f"开始多表比较: {len(self.table_pairs)} 组表对")
            self._run_multi_table_comparison(max_threads)

        self._mark_realtime_report_completed()
        elapsed_time = time.time() - start_time
        html_path = self._generate_html_report(elapsed_time)
        self._print_summary(elapsed_time, html_path)
        return self.all_results, html_path

    def _init_realtime_report(self):
        """初始化实时报告文件"""
        if not self.realtime_report_path:
            return

        report_data = {
            "start_time": self.report_start_time,
            "source_db": self.db1_url.split("://")[0],
            "target_db": self.db2_url.split("://")[0],
            "total_tables": self.total_tables,
            "completed_tables": 0,
            "results": [],
            "summary": {
                "consistent": 0,
                "inconsistent": 0,
                "errors": 0,
                "warnings": 0,
                "unmatched": 0
            },
            "status": "processing"
        }

        try:
            with open(self.realtime_report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            logger.info(f"实时报告已初始化: {self.realtime_report_path}")
        except Exception as e:
            logger.error(f"初始化实时报告失败: {str(e)}")

    def _update_realtime_report(self):
        """更新实时报告文件"""
        if not self.realtime_report_path or not os.path.exists(self.realtime_report_path):
            return

        try:
            with open(self.realtime_report_path, 'r', encoding='utf-8') as f:
                report_data = json.load(f)

            report_data["completed_tables"] = self.completed_tables
            report_data["total_tables"] = self.total_tables
            report_data["progress"] = f"{self.completed_tables}/{self.total_tables}"

            summary_keys = ["consistent", "inconsistent", "errors", "warnings", "unmatched"]
            for key in summary_keys:
                report_data["summary"][key] = 0

            for result in self.all_results:
                status = result.get("状态", "")
                if "[一致]" in status:
                    report_data["summary"]["consistent"] += 1
                elif "[不一致]" in status:
                    report_data["summary"]["inconsistent"] += 1
                elif "[错误]" in status or "[比对出错]" in status:
                    report_data["summary"]["errors"] += 1
                elif "[警告]" in status:
                    report_data["summary"]["warnings"] += 1
                elif "[表未匹配]" in status:
                    report_data["summary"]["unmatched"] += 1

            report_data["results"] = [
                {
                    "source_table": r.get("源库表名", ""),
                    "target_table": r.get("目标库表名", ""),
                    "status": r.get("状态", ""),
                    "key_info": r.get("键情况", {}),
                    "diff_detail": r.get("差异详情", ""),
                    "missing_rows": r.get("缺失行", 0),
                    "added_rows": r.get("新增行", 0),
                    "total_diffs": r.get("总差异数", 0),
                    "error_detail": r.get("错误详情", "")
                } for r in self.all_results
            ]

            with open(self.realtime_report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)

            logger.debug(f"实时报告已更新: {self.completed_tables}/{self.total_tables} 张表已完成")
        except Exception as e:
            logger.error(f"更新实时报告失败: {str(e)}")

    def _mark_realtime_report_completed(self):
        """标记实时报告为完成状态"""
        if not self.realtime_report_path or not os.path.exists(self.realtime_report_path):
            return

        try:
            with open(self.realtime_report_path, 'r', encoding='utf-8') as f:
                report_data = json.load(f)

            report_data["status"] = "completed"
            report_data["completion_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(self.realtime_report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)

            logger.info("实时报告标记为完成")
        except Exception as e:
            logger.error(f"标记实时报告为完成失败: {str(e)}")

    def _run_multi_table_comparison(self, max_threads):
        """执行多表比较"""
        self.total_tables = len(self.table_pairs)
        self.completed_tables = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for src_table, tgt_table in self.table_pairs:
                # 获取此表对的配置
                src_keys = self.key_mappings.get(src_table, ("", ""))[0] if src_table in self.key_mappings else ""
                tgt_keys = self.key_mappings.get(src_table, ("", ""))[1] if src_table in self.key_mappings else ""
                src_extra = self.extra_columns_map.get(src_table, ("", ""))[0] if src_table in self.extra_columns_map else ""
                tgt_extra = self.extra_columns_map.get(src_table, ("", ""))[1] if src_table in self.extra_columns_map else ""
                src_where = self.where_map.get(src_table, (None, None))[0] if src_table in self.where_map else None
                tgt_where = self.where_map.get(src_table, (None, None))[1] if src_table in self.where_map else None

                # 创建单表报告器
                reporter = SingleTableDiffReporter(
                    db1_url=self.db1_url,
                    db2_url=self.db2_url,
                    source_table_name=src_table,
                    target_table_name=tgt_table,
                    source_key_columns=src_keys,
                    target_key_columns=tgt_keys,
                    source_extra_columns=src_extra,
                    target_extra_columns=tgt_extra,
                    source_where=src_where,
                    target_where=tgt_where
                )
                futures.append(executor.submit(reporter.execute_comparison))

            # 处理完成的结果
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    self.all_results.append(result)
                    self.completed_tables += 1
                    self._update_realtime_report()
                except Exception as e:
                    logger.error(f"表比较失败: {str(e)}")
                    error_result = {
                        "源库表名": src_table if 'src_table' in locals() else "未知",
                        "目标库表名": tgt_table if 'tgt_table' in locals() else "未知",
                        "状态": "[错误] 比对异常",
                        "键情况": {
                            "key_columns": f"源键列: {src_keys or '无'}, 目标键列: {tgt_keys or '无'}",
                            "extra_columns": f"额外列: 源->{src_extra or '无'}, 目标->{tgt_extra or '无'}",
                            "where": f"过滤条件: 源->{src_where or '无'}, 目标->{tgt_where or '无'}"
                        },
                        "差异详情": f"比较异常: {str(e)}",
                        "缺失行": 0,
                        "新增行": 0,
                        "总差异数": 0,
                        "错误详情": traceback.format_exc()
                    }
                    self.all_results.append(error_result)
                    self.completed_tables += 1
                    self._update_realtime_report()

    def _run_single_table_comparison(self):
        """执行单表比较"""
        self.total_tables = 1
        reporter = SingleTableDiffReporter(
            db1_url=self.db1_url,
            db2_url=self.db2_url,
            source_table_name=self.source_table_name,
            target_table_name=self.target_table_name,
            source_key_columns=self.source_key_columns,
            target_key_columns=self.target_key_columns,
            source_extra_columns=self.source_extra_columns,
            target_extra_columns=self.target_extra_columns,
            source_where=self.source_where,
            target_where=self.target_where
        )
        result = reporter.execute_comparison()
        self.all_results = [result]
        self.completed_tables = 1
        self._update_realtime_report()

    def _run_full_database_comparison(self, max_threads, start_time):
        """执行全库比较"""
        src_inspector = DatabaseInspector(self.db1_url)
        tgt_inspector = DatabaseInspector(self.db2_url)

        src_tables = src_inspector.get_all_tables()
        tgt_tables = tgt_inspector.get_all_tables()

        logger.info(f"源数据库表数: {len(src_tables)}, 目标数据库表数: {len(tgt_tables)}")

        tgt_table_lower_map = {table.lower(): table for table in tgt_tables}

        matched_tables = []
        unmatched_tables = []

        for src_table in src_tables:
            if src_table.lower() in tgt_table_lower_map:
                matched_tables.append((src_table, tgt_table_lower_map[src_table.lower()]))
            else:
                unmatched_tables.append(src_table)

        logger.info(f"匹配表数: {len(matched_tables)}, 未匹配表数: {len(unmatched_tables)}")

        self.total_tables = len(matched_tables) + len(unmatched_tables)
        self.completed_tables = 0

        batch_size = 32
        total_batches = (len(matched_tables) + batch_size - 1) // batch_size

        for batch_index in range(total_batches):
            start_idx = batch_index * batch_size
            end_idx = min((batch_index + 1) * batch_size, len(matched_tables))
            current_batch = matched_tables[start_idx:end_idx]

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                future_to_task = {}

                for src_table, tgt_table in current_batch:
                    table_result = {
                        "源库表名": src_table,
                        "目标库表名": tgt_table,
                        "状态": "",
                        "键情况": {},
                        "差异详情": "",
                        "缺失行": 0,
                        "新增行": 0,
                        "总差异数": 0
                    }
                    self.all_results.append(table_result)

                    src_keys, src_key_type = src_inspector.get_table_keys(src_table)
                    tgt_keys, tgt_key_type = tgt_inspector.get_table_keys(tgt_table)

                    # 创建自动检测键的报告器
                    reporter = SingleTableDiffReporter(
                        db1_url=self.db1_url,
                        db2_url=self.db2_url,
                        source_table_name=src_table,
                        target_table_name=tgt_table,
                        source_key_columns=src_keys,
                        target_key_columns=tgt_keys
                    )
                    future = executor.submit(reporter.execute_comparison)
                    future_to_task[future] = (src_table, tgt_table, table_result)

                for future in concurrent.futures.as_completed(future_to_task):
                    src_table, tgt_table, table_result = future_to_task[future]
                    try:
                        result = future.result()
                        table_result.update(result)
                        self.completed_tables += 1
                        self._update_realtime_report()
                    except Exception as e:
                        table_result["状态"] = "[比对出错]"
                        table_result["差异详情"] = f"比较失败: {str(e)}"
                        table_result["错误详情"] = traceback.format_exc()
                        self.completed_tables += 1
                        self._update_realtime_report()

        # 处理未匹配表
        for src_table in unmatched_tables:
            self.all_results.append({
                "源库表名": src_table,
                "目标库表名": "未找到匹配表",
                "状态": "[表未匹配]",
                "键情况": {
                    "key_columns": "无",
                    "extra_columns": "无",
                    "where": "无"
                },
                "差异详情": "未在目标库中找到匹配的表",
                "缺失行": 0,
                "新增行": 0,
                "总差异数": 0,
                "错误详情": ""
            })
            self.completed_tables += 1
            self._update_realtime_report()

        src_inspector.close()
        tgt_inspector.close()

    def _generate_html_report(self, elapsed_time):
        """生成HTML差异报告"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        html_filename = f"db_diff_report_{timestamp}.html"

        def hide_password(url):
            """安全隐藏URL中的密码"""
            if "://" in url:
                scheme, rest = url.split("://", 1)
                if "@" in rest:
                    userinfo, host_part = rest.split("@", 1)
                    if ":" in userinfo:
                        username = userinfo.split(":", 1)[0]
                        return f"{scheme}://{username}:******@{host_part}"
            return url

        safe_db1_url = hide_password(self.db1_url)
        safe_db2_url = hide_password(self.db2_url)

        # 状态颜色映射
        status_colors = {
            "[一致]": "#d4edda",
            "[不一致]": "#f8d7da",
            "[比对出错]": "#f8d7da",
            "[错误]": "#f8d7da",
            "[警告]": "#fff3cd",
            "[表未匹配]": "#cce5ff"
        }

        # 计算统计信息
        matched_tables = []
        unmatched_count = 0
        consistent_count = 0
        inconsistent_count = 0
        error_count = 0
        warning_count = 0

        for result in self.all_results:
            if result["目标库表名"] == "未找到匹配表":
                unmatched_count += 1
            elif "[一致]" in result["状态"]:
                consistent_count += 1
                matched_tables.append((result["源库表名"], result["目标库表名"]))
            elif "[不一致]" in result["状态"]:
                inconsistent_count += 1
                matched_tables.append((result["源库表名"], result["目标库表名"]))
            elif "[警告]" in result["状态"]:
                warning_count += 1
            elif "[错误]" in result["状态"] or "[比对出错]" in result["状态"]:
                error_count += 1

        # 构建HTML内容
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>数据库比对报告 - {time.strftime("%Y-%m-%d %H:%M")}</title>
    <style>
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; }}
        .header {{ background-color: #2c3e50; color: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .summary {{ background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; padding: 15px; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
        th {{ background-color: #2c3e50; color: white; padding: 12px 15px; text-align: left; }}
        td {{ padding: 10px 15px; border-bottom: 1px solid #dee2e6; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        tr:hover {{ background-color: #e9ecef; }}
        .key-info {{ font-size: 0.9em; }}
        .status-cell {{ font-weight: bold; text-align: center; }}
        .timestamp {{ text-align: right; font-size: 0.9em; color: #6c757d; margin-top: 20px; }}
        .section-title {{ color: #2c3e50; border-bottom: 2px solid #2c3e50; padding-bottom: 8px; }}
        .security-note {{ background-color: #fff8e1; border-left: 4px solid #ffc107; padding: 10px; margin: 10px 0; }}
        .summary-table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        .summary-table td, .summary-table th {{ padding: 8px 12px; border: 1px solid #dee2e6; }}
        .summary-table th {{ background-color: #e9ecef; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>数据库比对报告</h1>
        <p>源数据库: {safe_db1_url}</p>
        <p>目标数据库: {safe_db2_url}</p>
        <p>生成时间: {time.strftime("%Y-%m-%d %H:%M:%S")}</p>
    </div>

    <div class="security-note">
        <strong>安全提示:</strong> 报告中已隐藏数据库密码信息以保护敏感数据
    </div>

    <div class="summary">
        <h2 class="section-title">统计概览</h2>
        <table class="summary-table">
            <tr>
                <th>指标</th>
                <th>数量</th>
                <th>说明</th>
            </tr>
            <tr>
                <td>总表数</td>
                <td>{len(self.all_results)}</td>
                <td>比对涉及的所有表数量</td>
            </tr>
            <tr>
                <td>匹配表数</td>
                <td>{len(matched_tables)}</td>
                <td>在目标数据库中找到匹配的表</td>
            </tr>
            <tr>
                <td>未匹配表数</td>
                <td>{unmatched_count}</td>
                <td>在目标数据库中找不到匹配的表</td>
            </tr>
            <tr>
                <td>一致表数</td>
                <td>{consistent_count}</td>
                <td>数据完全一致的表</td>
            </tr>
            <tr>
                <td>不一致表数</td>
                <td>{inconsistent_count}</td>
                <td>存在数据差异的表</td>
            </tr>
            <tr>
                <td>警告表数</td>
                <td>{warning_count}</td>
                <td>存在键列问题跳过的表</td>
            </tr>
            <tr>
                <td>出错表数</td>
                <td>{error_count}</td>
                <td>比对过程中出错或键无效的表</td>
            </tr>
            <tr>
                <td>总执行时间</td>
                <td colspan="2">{elapsed_time:.2f}秒</td>
            </tr>
        </table>
    </div>

    <h2 class="section-title">详细比对结果</h2>
    <table>
        <thead>
            <tr>
                <th>源库表名</th>
                <th>目标库表名</th>
                <th>状态</th>
                <th>键情况</th>
                <th>差异详情</th>
                <th>缺失行</th>
                <th>新增行</th>
                <th>总差异数</th>
            </tr>
        </thead>
        <tbody>
"""

        # 添加表行
        for result in self.all_results:
            status = result.get("状态", "[未知]")
            bg_color = status_colors.get(status, "#ffffff")
            key_info = result.get("键情况", {})

            html_content += f"""
            <tr>
                <td>{result.get("源库表名", "")}</td>
                <td>{result.get("目标库表名", "")}</td>
                <td class="status-cell" style="background-color:{bg_color}">{status}</td>
                <td>
                    <div class="key-info"><strong>键列:</strong> {key_info.get('key_columns', '')}</div>
                    <div class="key-info"><strong>额外列:</strong> {key_info.get('extra_columns', '')}</div>
                    <div class="key-info"><strong>过滤条件:</strong> {key_info.get('where', '')}</div>
                </td>
                <td>{result.get("差异详情", "")}</td>
                <td>{result.get("缺失行", 0)}</td>
                <td>{result.get("新增行", 0)}</td>
                <td>{result.get("总差异数", 0)}</td>
            </tr>
            """

        html_content += f"""
        </tbody>
    </table>

    <div class="timestamp">
        报告生成时间: {time.strftime("%Y-%m-%d %H:%M:%S")}
    </div>
</body>
</html>
"""

        # 保存HTML文件
        try:
            with open(html_filename, "w", encoding="utf-8") as f:
                f.write(html_content)
            return os.path.abspath(html_filename)
        except Exception as e:
            logger.error(f"保存HTML报告失败: {str(e)}")
            return None

    def _print_summary(self, elapsed_time, html_path):
        """打印比较摘要"""
        print("\n" + "=" * 80)
        print(f"数据库比对完成! 总耗时: {elapsed_time:.2f}秒")
        print("=" * 80)

        # 计算统计信息
        total_tables = len(self.all_results)
        matched_tables = sum(1 for r in self.all_results if r["目标库表名"] != "未找到匹配表")
        unmatched_count = sum(1 for r in self.all_results if r["目标库表名"] == "未找到匹配表")
        consistent_count = sum(1 for r in self.all_results if r.get("状态") == "[一致]")
        inconsistent_count = sum(1 for r in self.all_results if r.get("状态") == "[不一致]")
        warning_count = sum(1 for r in self.all_results if "[警告]" in r.get("状态", ""))
        error_count = sum(
            1 for r in self.all_results if "[错误]" in r.get("状态", "") or "[比对出错]" in r.get("状态", ""))

        print(f"统计摘要:")
        print(f"  总表数: {total_tables}")
        print(f"  匹配表数: {matched_tables}")
        print(f"  未匹配表数: {unmatched_count}")
        print(f"  一致表数: {consistent_count}")
        print(f"  不一致表数: {inconsistent_count}")
        print(f"  警告表数: {warning_count}")
        print(f"  出错表数: {error_count}")
        print("\n" + "=" * 80)
        if html_path:
            print(f"已生成详细的HTML报告: {html_path}")
        else:
            print("HTML报告生成失败")
        print("=" * 80)
