import importlib
import json
import os
import logging
import traceback
import concurrent.futures
import time
from datetime import datetime
from urllib.parse import urlparse

from data_diff import connect_to_table, diff_tables

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseInspector:
    """数据库元数据检查器，用于获取表信息和主键/唯一键信息"""

    def __init__(self, db_url):
        self.db_url = db_url
        self.db_type = self._get_db_type()
        self.schema = None  # 用于存储schema信息
        self.connection = self._create_connection()
        self.cursor = self.connection.cursor()

    def _get_db_type(self):
        """从URL中提取数据库类型"""
        db_type = self.db_url.split("://")[0].lower()
        return db_type

    def _create_connection(self):
        """创建数据库连接并提取schema信息"""
        import re  # 添加正则表达式支持

        # 使用正则表达式安全解析包含特殊字符的URL
        pattern = r'^(\w+)://([^:]+):([^@]+)@([^:/]+):(\d+)/([^/]+)/(.*)$'
        match = re.match(pattern, self.db_url)

        if match:
            # 使用正则表达式提取组件
            db_type = match.group(1)
            username = match.group(2)
            password = match.group(3)
            host = match.group(4)
            port = int(match.group(5))
            database = match.group(6)
            schema_part = match.group(7)

            # 解析schema部分（可能包含额外路径）
            schema_parts = schema_part.split('/')
            schema = schema_parts[0] if schema_parts else None
        else:
            # 尝试使用标准解析作为备选
            parsed = urlparse(self.db_url)
            username = parsed.username
            password = parsed.password
            host = parsed.hostname
            port = parsed.port

            # 解析路径部分
            path_parts = parsed.path.strip('/').split('/')
            database = path_parts[0] if path_parts else None
            schema = path_parts[1] if len(path_parts) > 1 else None

        # 处理schema
        if self.db_type == 'postgresql':
            self.schema = schema if schema else 'public'  # 默认schema为public
        elif self.db_type == 'mssql':
            self.schema = schema if schema else 'dbo'  # 默认schema为dbo
        elif self.db_type == 'oracle':
            # 使用传入的schema或username.upper()
            if schema:
                self.schema = schema.upper()  # 传入参数大写
            else:
                self.schema = username.upper() if username else None  # 默认用户名大写

        # 驱动映射表
        drivers = {
            'postgresql': ('psycopg2', None),
            'mysql': ('mysql.connector', 'mysql-connector-python'),
            'oracle': ('oracledb', 'oracledb'),
            'mssql': ('pyodbc', 'pyodbc')
        }

        # 获取驱动信息
        driver_module, package_name = drivers.get(self.db_type, (None, None))
        if not driver_module:
            raise ValueError(f"不支持的数据库类型: {self.db_type}")

        # 动态导入驱动
        try:
            module = importlib.import_module(driver_module)
        except ImportError:
            if package_name:
                raise ImportError(f"请安装数据库驱动: pip install {package_name}")
            raise

        # 根据数据库类型创建连接
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
        """获取数据库中的所有表名（保持原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                # 添加schema条件
                self.cursor.execute(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{self.schema}'
                    AND table_type = 'BASE TABLE'
                    order by table_name
                """)
            elif self.db_type == 'mysql':
                self.cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = DATABASE()
                    AND table_type = 'BASE TABLE'
                    order by table_name
                """)
            elif self.db_type == 'mssql':
                # 添加TABLE_SCHEMA条件
                self.cursor.execute(f"""
                    SELECT TABLE_NAME 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA = '{self.schema}'
                    order by TABLE_NAME
                """)
            elif self.db_type == 'oracle':
                self.cursor.execute(f"""
                    SELECT table_name 
                    FROM dba_tables 
                    WHERE owner = '{self.schema}'
                    order by table_name
                """)
            else:
                raise ValueError(f"不支持的数据库类型: {self.db_type}")

            return [row[0] for row in self.cursor.fetchall()]
        except Exception as e:
            logger.error(f"获取表列表失败: {str(e)}")
            return []

    def get_table_columns(self, table_name):
        """获取表的所有列名（保持原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                # 添加schema条件
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
                # 添加TABLE_SCHEMA条件
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
            logger.error(f"获取表 {table_name} 列名失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return []

    def get_table_keys(self, table_name):
        """获取表的主键或唯一键（保持原始大小写）"""
        primary_keys = self._get_primary_keys(table_name)
        if primary_keys:
            return primary_keys, "主键"

        unique_keys = self._get_unique_keys(table_name)
        if unique_keys:
            return unique_keys, "唯一键"

        return "", None  # 返回空字符串

    def _get_primary_keys(self, table_name):
        """获取主键列（保持原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                # 添加schema条件
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
                try:
                    # 使用SHOW INDEX获取主键
                    self.cursor.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'PRIMARY'")
                    rows = self.cursor.fetchall()
                    if rows:
                        # 获取列名索引
                        column_name_idx = self._get_column_index(self.cursor.description, 'Column_name')
                        seq_idx = self._get_column_index(self.cursor.description, 'Seq_in_index')

                        # 按Seq_in_index排序
                        rows.sort(key=lambda row: row[seq_idx])
                        primary_keys = [row[column_name_idx] for row in rows]
                        return ",".join(primary_keys)
                    else:
                        return ""
                except Exception as e:
                    logger.error(f"SHOW INDEX获取主键失败: {str(e)}")
            elif self.db_type == 'mssql':
                # 添加TABLE_SCHEMA条件
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
            logger.error(f"获取表 {table_name} 主键失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return ""

    def _get_unique_keys(self, table_name):
        """获取唯一键列（保持原始大小写）"""
        try:
            if self.db_type == 'postgresql':
                # 添加schema条件
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
                try:
                    # 使用SHOW INDEX获取唯一键
                    self.cursor.execute(
                        f"SHOW INDEX FROM `{table_name}` WHERE Non_unique = 0 AND Key_name != 'PRIMARY'")
                    rows = self.cursor.fetchall()
                    if rows:
                        # 获取列名索引
                        column_name_idx = self._get_column_index(self.cursor.description, 'Column_name')
                        key_name_idx = self._get_column_index(self.cursor.description, 'Key_name')
                        seq_idx = self._get_column_index(self.cursor.description, 'Seq_in_index')

                        # 按Key_name和Seq_in_index排序
                        rows.sort(key=lambda row: (row[key_name_idx], row[seq_idx]))

                        # 分组处理唯一键
                        unique_constraints = {}
                        for row in rows:
                            key_name = row[key_name_idx]
                            col_name = row[column_name_idx]
                            if key_name not in unique_constraints:
                                unique_constraints[key_name] = []
                            unique_constraints[key_name].append(col_name)

                        # 取第一个唯一键的列
                        if unique_constraints:
                            first_key = sorted(unique_constraints.keys())[0]
                            return ",".join(unique_constraints[first_key])
                        else:
                            return ""
                    else:
                        return ""
                except Exception as e:
                    logger.error(f"SHOW INDEX获取唯一键失败: {str(e)}")
            elif self.db_type == 'mssql':
                # 添加TABLE_SCHEMA条件
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
            logger.error(f"获取表 {table_name} 唯一键失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return ""

    def _get_column_index(self, description, column_name):
        """获取结果集中指定列的索引位置"""
        for idx, col in enumerate(description):
            if col[0].lower() == column_name.lower():
                return idx
        # 如果找不到，尝试使用已知的位置
        if column_name.lower() == 'column_name':
            return 4  # MySQL SHOW INDEX中Column_name通常是第5列(索引4)
        elif column_name.lower() == 'key_name':
            return 2  # MySQL SHOW INDEX中Key_name通常是第3列(索引2)
        elif column_name.lower() == 'seq_in_index':
            return 3  # MySQL SHOW INDEX中Seq_in_index通常是第4列(索引3)
        return 0  # 默认返回第一列

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

class TableDiffReporter:
    """表差异报告器，支持全库、单表和多表比对，支持实时报告"""

    def __init__(self, db1_url, db2_url,
                 source_table_name=None, target_table_name=None,
                 source_key_columns=None, target_key_columns=None,
                 extra_columns=None,
                 table_pairs=None, key_mappings=None, extra_columns_map=None,
                 realtime_report_path=None):
        """
        初始化表差异报告器（支持实时报告）

        :param db1_url: 源数据库连接URL
        :param db2_url: 目标数据库连接URL
        :param source_table_name: 源表名（原始大小写）
        :param target_table_name: 目标表名（原始大小写）
        :param source_key_columns: 键列（单表模式为字符串，多表模式为列表）
        :param target_key_columns: 键列（单表模式为字符串，多表模式为列表）
        :param extra_columns: 额外列名（单表模式为列表）
        :param table_pairs: 多表比对模式下的表对列表
        :param key_mappings: 多表比对模式下的键列映射
        :param extra_columns_map: 多表比对模式下的额外列映射
        :param realtime_report_path: 实时报告文件路径（可选）
        """
        self.db1_url = db1_url
        self.db2_url = db2_url
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.source_key_columns = source_key_columns
        self.target_key_columns = target_key_columns
        self.extra_columns = extra_columns or []
        self.table_pairs = table_pairs or []
        self.key_mappings = key_mappings or {}
        self.extra_columns_map = extra_columns_map or {}
        self.realtime_report_path = realtime_report_path

        # 结果存储
        self.all_results = []
        self.start_time = time.time()
        self.report_start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.total_tables = 0
        self.completed_tables = 0

        # 自动检测比对模式
        self.mode = self._detect_mode()
        logger.info(f"检测到比对模式: {self.mode}")

    def _detect_mode(self):
        """自动检测比对模式"""
        # 1. 显式表对模式：提供了table_pairs
        if self.table_pairs:
            logger.info(f"检测到多表比对模式（显式表对）: {len(self.table_pairs)}张表")
            return "multi_table"

        # 2. 列表形式多表模式：source_table_name和target_table_name都是列表
        if (isinstance(self.source_table_name, list) and
                isinstance(self.target_table_name, list)):
            if len(self.source_table_name) != len(self.target_table_name):
                raise ValueError("源表和目标表数量不匹配")

            # 自动转换为表对格式
            self.table_pairs = list(zip(self.source_table_name, self.target_table_name))

            # 自动转换键列映射
            if (isinstance(self.source_key_columns, list) and
                    isinstance(self.target_key_columns, list)):
                if (len(self.source_table_name) != len(self.source_key_columns) or
                        len(self.source_table_name) != len(self.target_key_columns)):
                    raise ValueError("表名列表与键列列表长度不匹配")

                self.key_mappings = {
                    src_table: (src_keys, tgt_keys)
                    for src_table, src_keys, tgt_keys in zip(
                        self.source_table_name, self.source_key_columns, self.target_key_columns
                    )
                }

            logger.info(f"检测到多表比对模式（列表形式）: {len(self.table_pairs)}张表")
            return "multi_table"

        # 3. 单表模式：source_table_name和target_table_name都是字符串
        if (isinstance(self.source_table_name, str) and
                isinstance(self.target_table_name, str)):
            logger.info(f"检测到单表比对模式: {self.source_table_name}->{self.target_table_name}")
            return "single_table"

        # 4. 全库模式：所有参数为空
        logger.info("检测到全库比对模式（所有参数为空）")
        return "full_db"

    def run_report(self, max_threads=5):
        """执行数据库比对并生成报告"""
        start_time = time.time()

        # 初始化实时报告
        self._init_realtime_report()

        # 根据模式执行对应比对
        if self.mode == "full_db":
            logger.info(f"开始全库比对: {self.db1_url} -> {self.db2_url}")
            self._run_full_database_comparison(max_threads, start_time)
        elif self.mode == "single_table":
            logger.info(f"开始单表比对: {self.source_table_name} -> {self.target_table_name}")
            self._run_single_table_comparison()
        elif self.mode == "multi_table":
            logger.info(f"开始多表比对: {len(self.table_pairs)} 对表")
            self._run_multi_table_comparison(max_threads)

        # 更新实时报告为完成状态
        self._mark_realtime_report_completed()

        # 生成最终HTML报告
        elapsed_time = time.time() - start_time
        html_path = self._generate_html_report(elapsed_time)

        # 输出摘要
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
            logger.info(f"实时报告初始化: {self.realtime_report_path}")
        except Exception as e:
            logger.error(f"初始化实时报告失败: {str(e)}")

    def _update_realtime_report(self):
        """更新实时报告文件"""
        if not self.realtime_report_path or not os.path.exists(self.realtime_report_path):
            return

        try:
            # 读取现有报告
            with open(self.realtime_report_path, 'r', encoding='utf-8') as f:
                report_data = json.load(f)

            # 更新进度
            report_data["completed_tables"] = self.completed_tables
            report_data["total_tables"] = self.total_tables
            report_data["progress"] = f"{self.completed_tables}/{self.total_tables}"

            # 重置摘要统计
            summary_keys = ["consistent", "inconsistent", "errors", "warnings", "unmatched"]
            for key in summary_keys:
                report_data["summary"][key] = 0

            # 更新摘要统计
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

            # 更新结果
            report_data["results"] = [
                {
                    "source_table": r.get("源库表名", ""),
                    "target_table": r.get("目标库表名", ""),
                    "status": r.get("状态", ""),
                    "key_info": r.get("键情况", ""),
                    "diff_detail": r.get("差异详情", ""),
                    "missing_rows": r.get("缺失行", 0),
                    "added_rows": r.get("新增行", 0),
                    "total_diffs": r.get("总差异数", 0),
                    "error_detail": r.get("错误详情", "")
                } for r in self.all_results
            ]

            # 保存更新
            with open(self.realtime_report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)

            logger.debug(f"实时报告已更新: {self.completed_tables}/{self.total_tables} 表完成")

        except Exception as e:
            logger.error(f"更新实时报告失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")

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

            logger.info("实时报告标记为完成状态")

        except Exception as e:
            logger.error(f"标记实时报告完成失败: {str(e)}")

    def _run_multi_table_comparison(self, max_threads):
        """执行多表比对（支持实时报告）"""
        self.total_tables = len(self.table_pairs)
        self.completed_tables = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = []
            for src_table, tgt_table in self.table_pairs:
                # 获取该表对的键列和额外列
                src_keys = self.key_mappings.get(src_table, ("", ""))[0] if src_table in self.key_mappings else ""
                tgt_keys = self.key_mappings.get(src_table, ("", ""))[1] if src_table in self.key_mappings else ""
                extra_cols = self.extra_columns_map.get(src_table, [])

                # 创建单表比对器
                reporter = SingleTableDiffReporter(
                    db1_url=self.db1_url,
                    db2_url=self.db2_url,
                    source_table_name=src_table,
                    target_table_name=tgt_table,
                    source_key_columns=src_keys,
                    target_key_columns=tgt_keys,
                    extra_columns=extra_cols
                )
                # 提交任务
                futures.append(executor.submit(reporter.execute_comparison))

            # 按完成顺序收集结果
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    self.all_results.append(result)
                    self.completed_tables += 1
                    self._update_realtime_report()
                except Exception as e:
                    logger.error(f"表对比失败: {str(e)}")
                    error_result = {
                        "源库表名": src_table if 'src_table' in locals() else "未知",
                        "目标库表名": tgt_table if 'tgt_table' in locals() else "未知",
                        "状态": "[错误] 比对异常",
                        "键情况": f"源键: {src_keys}, 目标键: {tgt_keys}",
                        "差异详情": f"比对异常: {str(e)}",
                        "缺失行": 0,
                        "新增行": 0,
                        "总差异数": 0,
                        "错误详情": traceback.format_exc()
                    }
                    self.all_results.append(error_result)
                    self.completed_tables += 1
                    self._update_realtime_report()

    def _run_single_table_comparison(self):
        """执行单表比对（支持实时报告）"""
        self.total_tables = 1
        reporter = SingleTableDiffReporter(
            db1_url=self.db1_url,
            db2_url=self.db2_url,
            source_table_name=self.source_table_name,
            target_table_name=self.target_table_name,
            source_key_columns=self.source_key_columns,
            target_key_columns=self.target_key_columns,
            extra_columns=self.extra_columns
        )
        result = reporter.execute_comparison()
        self.all_results = [result]
        self.completed_tables = 1
        self._update_realtime_report()

    def _run_full_database_comparison(self, max_threads, start_time):
        """执行全库比对（分批处理，支持实时报告）"""
        # 创建数据库检查器
        src_inspector = DatabaseInspector(self.db1_url)
        tgt_inspector = DatabaseInspector(self.db2_url)

        # 获取表列表（保持原始大小写）
        src_tables = src_inspector.get_all_tables()
        tgt_tables = tgt_inspector.get_all_tables()

        logger.info(f"源数据库表数量: {len(src_tables)}, 目标数据库表数量: {len(tgt_tables)}")

        # 创建目标表的小写映射
        tgt_table_lower_map = {table.lower(): table for table in tgt_tables}

        # 找出匹配的表
        matched_tables = []
        unmatched_tables = []

        for src_table in src_tables:
            if src_table.lower() in tgt_table_lower_map:
                matched_tables.append((src_table, tgt_table_lower_map[src_table.lower()]))
            else:
                unmatched_tables.append(src_table)

        logger.info(f"匹配的表数量: {len(matched_tables)}, 未匹配的表数量: {len(unmatched_tables)}")

        # 存储所有表的结果
        self.total_tables = len(matched_tables) + len(unmatched_tables)
        self.completed_tables = 0

        # 分批处理配置
        batch_size = 32
        total_batches = (len(matched_tables) + batch_size - 1) // batch_size
        logger.info(f"开始全库比对，将分 {total_batches} 批处理, 每批 {batch_size} 个表")

        # 处理匹配的表（分批）
        for batch_index in range(total_batches):
            start_idx = batch_index * batch_size
            end_idx = min((batch_index + 1) * batch_size, len(matched_tables))
            current_batch = matched_tables[start_idx:end_idx]

            logger.info(f"处理批次 {batch_index + 1}/{total_batches}: 表 {start_idx + 1}-{end_idx}")

            # 使用线程池执行当前批次的任务
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                future_to_task = {}

                # 提交当前批次的所有任务
                for src_table, tgt_table in current_batch:
                    table_result = {
                        "源库表名": src_table,
                        "目标库表名": tgt_table,
                        "状态": "",
                        "键情况": "",
                        "差异详情": "",
                        "缺失行": 0,
                        "新增行": 0,
                        "总差异数": 0
                    }
                    self.all_results.append(table_result)

                    # 获取键信息
                    src_keys, src_key_type = src_inspector.get_table_keys(src_table)
                    tgt_keys, tgt_key_type = tgt_inspector.get_table_keys(tgt_table)

                    # 检查键是否存在
                    src_has_keys = bool(src_keys and src_keys.strip())
                    tgt_has_keys = bool(tgt_keys and tgt_keys.strip())

                    # 获取列信息
                    src_columns = src_inspector.get_table_columns(src_table)
                    tgt_columns = tgt_inspector.get_table_columns(tgt_table)

                    if not src_has_keys:
                        table_result["键情况"] = "源表无主键和唯一键"

                    if not tgt_has_keys:
                        table_result["键情况"] += "，目标表无主键和唯一键"

                    if not src_has_keys and not tgt_has_keys:
                        table_result["状态"] = "[警告] 双方无键"
                        table_result["差异详情"] = "跳过比对"
                        self.completed_tables += 1
                        self._update_realtime_report()
                        continue

                    # 确定键列
                    if src_has_keys:
                        source_keys = src_keys
                        table_result["键情况"] = f"源表{src_key_type}: {src_keys}"
                    else:
                        target_key_list = [k.strip() for k in tgt_keys.split(',') if k.strip()]
                        matched_src_keys = find_matching_columns(src_columns, target_key_list)
                        if matched_src_keys:
                            source_keys = ",".join(matched_src_keys)
                            table_result["键情况"] = f"源表无键，使用目标表{tgt_key_type}匹配列"
                        else:
                            source_keys = ""
                            table_result["键情况"] = f"源表无键，无法匹配目标表键列"

                    if tgt_has_keys:
                        target_keys = tgt_keys
                        table_result["键情况"] += f"，目标表{tgt_key_type}: {tgt_keys}"
                    else:
                        source_key_list = [k.strip() for k in src_keys.split(',') if k.strip()]
                        matched_tgt_keys = find_matching_columns(tgt_columns, source_key_list)
                        if matched_tgt_keys:
                            target_keys = ",".join(matched_tgt_keys)
                            table_result["键情况"] += f"，目标表借用源表{src_key_type}匹配列"
                        else:
                            target_keys = ""
                            table_result["键情况"] += f"，目标表无法匹配源表键列"

                    # 检查键有效性
                    if not source_keys.strip() or not target_keys.strip():
                        table_result["状态"] = "[错误] 无有效键列"
                        table_result["差异详情"] = "跳过比对"
                        self.completed_tables += 1
                        self._update_realtime_report()
                        continue

                    source_key_list = [k.strip() for k in source_keys.split(',') if k.strip()]
                    target_key_list = [k.strip() for k in target_keys.split(',') if k.strip()]

                    if len(source_key_list) != len(target_key_list):
                        table_result["状态"] = "[警告] 键列数不匹配"
                        table_result["差异详情"] = "跳过比对"
                        self.completed_tables += 1
                        self._update_realtime_report()
                        continue

                    source_key_set = set(k.lower() for k in source_key_list)
                    target_key_set = set(k.lower() for k in target_key_list)

                    if source_key_set != target_key_set:
                        table_result["状态"] = "[警告] 键列名不匹配"
                        table_result["键情况"] = f"源表键列名 ≠ 目标表键列名"

                    # 创建比对任务
                    reporter = SingleTableDiffReporter(
                        db1_url=self.db1_url,
                        db2_url=self.db2_url,
                        source_table_name=src_table,
                        target_table_name=tgt_table,
                        source_key_columns=source_keys,
                        target_key_columns=target_keys
                    )
                    # 提交任务
                    future = executor.submit(reporter.execute_comparison)
                    future_to_task[future] = (src_table, tgt_table, table_result)

                # 按完成顺序处理结果
                for future in concurrent.futures.as_completed(future_to_task):
                    src_table, tgt_table, table_result = future_to_task[future]
                    try:
                        result = future.result()
                        # 更新结果对象
                        table_result.update(result)
                        self.completed_tables += 1
                        self._update_realtime_report()
                    except Exception as e:
                        table_result["状态"] = "[比对出错]"
                        err_msg = str(e)
                        # 截断过长的错误信息
                        if len(err_msg) > 120:
                            err_msg = err_msg[:117] + "..."
                        table_result["差异详情"] = f"比对过程中出错: {err_msg}"
                        # 记录完整错误信息
                        table_result["错误详情"] = traceback.format_exc()
                        self.completed_tables += 1
                        self._update_realtime_report()

            logger.info(f"批次 {batch_index + 1}/{total_batches} 比对完成")

        # 处理未匹配的表（不分批）
        for src_table in unmatched_tables:
            self.all_results.append({
                "源库表名": src_table,
                "目标库表名": "未找到匹配表",
                "状态": "[表未匹配]",
                "键情况": "",
                "差异详情": "未在目标库中找到匹配的表",
                "缺失行": 0,
                "新增行": 0,
                "总差异数": 0,
                "错误详情": ""
            })
            self.completed_tables += 1
            self._update_realtime_report()

        # 关闭检查器
        src_inspector.close()
        tgt_inspector.close()


    def _generate_html_report(self, elapsed_time):
        """生成HTML差异报告"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        html_filename = f"db_diff_report_{timestamp}.html"

        # 状态颜色映射
        status_colors = {
            "[一致]": "#d4edda",
            "[不一致]": "#f8d7da",
            "[比对出错]": "#f8d7da",
            "[错误] 无有效键列": "#f8d7da",
            "[错误] 源表不存在": "#f8d7da",
            "[错误] 目标表不存在": "#f8d7da",
            "[警告]": "#fff3cd",
            "[表未匹配]": "#cce5ff"
        }

        # 隐藏URL中的密码
        def hide_password(url):
            """安全隐藏URL中的密码（支持特殊字符）"""
            import re

            try:
                # 使用正则表达式安全解析包含特殊字符的URL
                pattern = r'^(\w+)://([^:]+):([^@]+)@([^:/]+):(\d+)/(.*)$'
                match = re.match(pattern, url)

                if match:
                    # 提取组件
                    scheme = match.group(1)
                    username = match.group(2)
                    host = match.group(4)
                    port = match.group(5)
                    path = match.group(6)

                    # 返回隐藏密码的URL
                    return f"{scheme}://{username}:******@{host}:{port}/{path}"

                # 备选方案：尝试标准解析
                parsed = urlparse(url)
                if parsed.password:
                    return f"{parsed.scheme}://{parsed.username}:******@{parsed.hostname}:{parsed.port}{parsed.path}"

                return url
            except Exception:
                # 最终备选：简单字符串处理
                if "://" in url:
                    scheme, rest = url.split("://", 1)
                    if "@" in rest:
                        parts = rest.split("@", 1)
                        userinfo = parts[0]
                        host_part = parts[1]

                        if ":" in userinfo:
                            username = userinfo.split(":", 1)[0]
                            return f"{scheme}://{username}:******@{host_part}"
                        else:
                            return f"{scheme}://******@{host_part}"
                return url

        # 隐藏密码的数据库URL
        safe_db1_url = hide_password(self.db1_url)
        safe_db2_url = hide_password(self.db2_url)

        # 计算统计数据
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
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>数据库比对报告 - {time.strftime("%Y-%m-%d %H:%M")}</title>
            <style>
                body {{ 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 20px;
                    color: #333;
                }}
                .header {{
                    background-color: #2c3e50;
                    color: white;
                    padding: 15px;
                    border-radius: 5px;
                    margin-bottom: 20px;
                }}
                .summary {{
                    background-color: #f8f9fa;
                    border: 1px solid #dee2e6;
                    border-radius: 5px;
                    padding: 15px;
                    margin-bottom: 20px;
                }}
                .table-container {{
                    overflow-x: auto;
                    margin-bottom: 30px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 20px;
                }}
                th {{
                    background-color: #2c3e50;
                    color: white;
                    padding: 12px 15px;
                    text-align: left;
                    font-weight: bold;
                }}
                td {{
                    padding: 10px 15px;
                    border-bottom: 1px solid #dee2e6;
                    word-break: break-all;
                }}
                tr:nth-child(even) {{
                    background-color: #f8f9fa;
                }}
                tr:hover {{
                    background-color: #e9ecef;
                }}
                .status-cell {{
                    font-weight: bold;
                    text-align: center;
                    min-width: 100px;
                }}
                .stat-value {{
                    font-weight: bold;
                    color: #2c3e50;
                }}
                .timestamp {{
                    text-align: right;
                    font-size: 0.9em;
                    color: #6c757d;
                    margin-top: 20px;
                }}
                h2 {{
                    color: #2c3e50;
                    border-bottom: 2px solid #2c3e50;
                    padding-bottom: 8px;
                }}
                .security-note {{
                    background-color: #fff8e1;
                    border-left: 4px solid #ffc107;
                    padding: 10px;
                    margin: 10px 0;
                    font-size: 0.9em;
                }}
                .summary-table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                }}
                .summary-table td, .summary-table th {{
                    padding: 8px 12px;
                    border: 1px solid #dee2e6;
                }}
                .summary-table th {{
                    background-color: #e9ecef;
                }}
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
                <strong>安全提示:</strong> 报告中已隐藏数据库密码信息以保护敏感数据。
            </div>

            <div class="summary">
                <h2>统计概览</h2>
                <table class="summary-table">
                    <tr>
                        <th>指标</th>
                        <th>数量</th>
                        <th>说明</th>
                    </tr>
                    <tr>
                        <td>总表数</td>
                        <td class="stat-value">{len(self.all_results)}</td>
                        <td>比对涉及的所有表数量</td>
                    </tr>
                    <tr>
                        <td>匹配表数</td>
                        <td class="stat-value">{len(matched_tables)}</td>
                        <td>在目标数据库中找到匹配的表</td>
                    </tr>
                    <tr>
                        <td>未匹配表数</td>
                        <td class="stat-value">{unmatched_count}</td>
                        <td>在目标数据库中找不到匹配的表</td>
                    </tr>
                    <tr>
                        <td>成功比对表数</td>
                        <td class="stat-value">{consistent_count + inconsistent_count}</td>
                        <td>完成比对的表数量</td>
                    </tr>
                    <tr>
                        <td><span style="background-color:{status_colors['[一致]']}">[一致]</span> 一致表数</td>
                        <td class="stat-value">{consistent_count}</td>
                        <td>数据完全一致的表</td>
                    </tr>
                    <tr>
                        <td><span style="background-color:{status_colors['[不一致]']}">[不一致]</span> 表数</td>
                        <td class="stat-value">{inconsistent_count}</td>
                        <td>存在数据差异的表</td>
                    </tr>
                    <tr>
                        <td><span style="background-color:{status_colors['[警告]']}">[警告]</span> 警告表数</td>
                        <td class="stat-value">{warning_count}</td>
                        <td>存在键列问题跳过的表</td>
                    </tr>
                    <tr>
                        <td><span style="background-color:{status_colors['[比对出错]']}">[出错]</span> 出错表数</td>
                        <td class="stat-value">{error_count}</td>
                        <td>比对过程中出错或键无效的表</td>
                    </tr>
                    <tr>
                        <td>总执行时间</td>
                        <td class="stat-value" colspan="2">{elapsed_time:.2f}秒</td>
                    </tr>
                </table>
            </div>

            <h2>详细比对结果</h2>
            <div class="table-container">
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

        # 添加表格行
        for result in self.all_results:
            # 安全获取状态，避免KeyError
            status = result.get("状态", "[未知]")
            bg_color = status_colors.get(status, "#ffffff")

            html_content += f"""
            <tr>
                <td>{result.get("源库表名", "")}</td>
                <td>{result.get("目标库表名", "")}</td>
                <td class="status-cell" style="background-color:{bg_color}">{status}</td>
                <td>{result.get("键情况", "")}</td>
                <td>{result.get("差异详情", "")}</td>
                <td>{result.get("缺失行", 0)}</td>
                <td>{result.get("新增行", 0)}</td>
                <td>{result.get("总差异数", 0)}</td>
            </tr>
            """

        html_content += f"""
                    </tbody>
                </table>
            </div>

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
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return None

    def _print_summary(self, elapsed_time, html_path):
        """输出比对摘要信息"""
        print("\n" + "=" * 80)
        print(f"数据库比对完成! 耗时: {elapsed_time:.2f}秒")
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
        print(f"  成功比对表数: {consistent_count + inconsistent_count}")
        print(f"  [一致] 一致表数: {consistent_count}")
        print(f"  [不一致] 表数: {inconsistent_count}")
        print(f"  [警告] 警告表数: {warning_count} (键列问题)")
        print(f"  [出错] 出错表数: {error_count} (比对过程中出错)")
        print("\n" + "=" * 80)
        if html_path:
            print(f"已生成详细的HTML报告: {html_path}")
            print(f"数据库密码信息已在报告中隐藏")
        else:
            print("HTML报告生成失败")
        print("=" * 80)


class SingleTableDiffReporter:
    """单表差异报告器"""

    def __init__(self, db1_url, db2_url, source_table_name, target_table_name,
                 source_key_columns, target_key_columns, extra_columns=None):
        """
        初始化单表差异报告器

        :param db1_url: 源数据库连接URL
        :param db2_url: 目标数据库连接URL
        :param source_table_name: 源表名（原始大小写）
        :param target_table_name: 目标表名（原始大小写）
        :param source_key_columns: 源表主键列名（逗号分隔，原始大小写）
        :param target_key_columns: 目标表主键列名（逗号分隔，原始大小写）
        :param extra_columns: 需要同步对比的额外列名（列表，原始大小写）
        """
        self.db1_url = db1_url
        self.db2_url = db2_url
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.source_key_columns = source_key_columns
        self.target_key_columns = target_key_columns
        self.extra_columns = extra_columns or []

        self.source = None
        self.target = None
        self.missing_rows = 0
        self.added_rows = 0
        self.error = None  # 存储错误信息

    def execute_comparison(self):
        """执行单表比对并返回结果"""
        try:
            # 创建数据库检查器
            src_inspector = DatabaseInspector(self.db1_url)
            tgt_inspector = DatabaseInspector(self.db2_url)

            # 存储数据库类型和schema
            self.src_db_type = src_inspector.db_type
            self.src_schema = src_inspector.schema
            self.tgt_db_type = tgt_inspector.db_type
            self.tgt_schema = tgt_inspector.schema

            # 检查表是否存在
            src_tables = src_inspector.get_all_tables()
            tgt_tables = tgt_inspector.get_all_tables()

            # 初始化结果字典
            result = {
                "源库表名": self.source_table_name,
                "目标库表名": self.target_table_name,
                "状态": "",
                "键情况": "",
                "差异详情": "",
                "缺失行": 0,
                "新增行": 0,
                "总差异数": 0,
                "错误详情": ""
            }

            # 检查源表是否存在
            if self.source_table_name not in src_tables:
                err_msg = f"源表 '{self.source_table_name}' 在源数据库中不存在"
                logger.error(err_msg)
                result.update({
                    "状态": "[错误] 源表不存在",
                    "键情况": "源表不存在",
                    "差异详情": err_msg,
                    "错误详情": err_msg
                })
                return result

            # 检查目标表是否存在
            if self.target_table_name not in tgt_tables:
                err_msg = f"目标表 '{self.target_table_name}' 在目标数据库中不存在"
                logger.error(err_msg)
                result.update({
                    "状态": "[错误] 目标表不存在",
                    "键情况": "目标表不存在",
                    "差异详情": err_msg,
                    "错误详情": err_msg
                })
                return result

            # 获取表键信息和列信息
            src_keys, src_key_type = src_inspector.get_table_keys(self.source_table_name)
            tgt_keys, tgt_key_type = tgt_inspector.get_table_keys(self.target_table_name)
            src_columns = src_inspector.get_table_columns(self.source_table_name)
            tgt_columns = tgt_inspector.get_table_columns(self.target_table_name)

            # 使用传入的键列或自动获取的键列
            actual_source_keys = self.source_key_columns or src_keys
            actual_target_keys = self.target_key_columns or tgt_keys

            # 处理键列缺失情况
            if not actual_source_keys or not actual_target_keys:
                logger.warning("键列不完整，尝试通过列名匹配")
                key_error = ""

                # 如果源表有键但目标表无键
                if src_keys and not actual_target_keys:
                    # 尝试在目标表中匹配源表键列名
                    source_key_list = [k.strip() for k in src_keys.split(',') if k.strip()]
                    matched_tgt_keys = find_matching_columns(tgt_columns, source_key_list)
                    if matched_tgt_keys:
                        actual_target_keys = ",".join(matched_tgt_keys)
                        logger.info(f"使用目标表匹配列: {actual_target_keys}")
                    else:
                        key_error += " - 目标表无法匹配源表键列"

                # 如果目标表有键但源表无键
                if tgt_keys and not actual_source_keys:
                    # 尝试在源表中匹配目标表键列名
                    target_key_list = [k.strip() for k in tgt_keys.split(',') if k.strip()]
                    matched_src_keys = find_matching_columns(src_columns, target_key_list)
                    if matched_src_keys:
                        actual_source_keys = ",".join(matched_src_keys)
                        logger.info(f"使用源表匹配列: {actual_source_keys}")
                    else:
                        key_error += " - 源表无法匹配目标表键列"

                # 如果双方都无有效键列
                if not actual_source_keys or not actual_target_keys:
                    if not key_error:
                        key_error = "无法确定有效键列"
                    if not actual_source_keys:
                        key_error += " - 源表无有效键列"
                    if not actual_target_keys:
                        key_error += " - 目标表无有效键列"

                    logger.error(key_error)
                    result.update({
                        "状态": "[错误] 无有效键列",
                        "键情况": f"源键: {actual_source_keys or '无'}, 目标键: {actual_target_keys or '无'}",
                        "差异详情": key_error,
                        "错误详情": key_error
                    })
                    return result

            # 更新实例的键列
            self.source_key_columns = actual_source_keys
            self.target_key_columns = actual_target_keys

            # 连接到表
            if not self._connect_to_tables():
                result.update({
                    "状态": "[错误] 连接失败",
                    "键情况": f"源键: {self.source_key_columns}, 目标键: {self.target_key_columns}",
                    "差异详情": self.error or "未知错误",
                    "错误详情": self.error
                })
                return result

            # 执行比对
            if not self._generate_diff():
                result.update({
                    "状态": "[错误] 比对失败",
                    "键情况": f"源键: {self.source_key_columns}, 目标键: {self.target_key_columns}",
                    "差异详情": self.error or "未知错误",
                    "错误详情": self.error
                })
                return result

            # 准备结果
            if self.missing_rows == 0 and self.added_rows == 0:
                status = "[一致]"
                diff_detail = "无差异"
            else:
                status = "[不一致]"
                diff_detail = f"缺失行: {self.missing_rows}, 新增行: {self.added_rows}"

            result.update({
                "状态": status,
                "键情况": f"源键列: {self.source_key_columns}, 目标键列: {self.target_key_columns}",
                "差异详情": diff_detail,
                "缺失行": self.missing_rows,
                "新增行": self.added_rows,
                "总差异数": self.missing_rows + self.added_rows
            })

            return result
        except Exception as e:
            logger.error(f"单表比对异常: {str(e)}")
            logger.debug(traceback.format_exc())
            return {
                "源库表名": self.source_table_name,
                "目标库表名": self.target_table_name,
                "状态": "[错误] 比对异常",
                "键情况": f"源键: {self.source_key_columns}, 目标键: {self.target_key_columns}",
                "差异详情": f"比对过程中发生异常: {str(e)}",
                "缺失行": 0,
                "新增行": 0,
                "总差异数": 0,
                "错误详情": traceback.format_exc()
            }
        finally:
            # 关闭检查器
            if 'src_inspector' in locals():
                src_inspector.close()
            if 'tgt_inspector' in locals():
                tgt_inspector.close()

    def _connect_to_tables(self):
        """连接到源数据库和目标数据库表（使用原始大小写）"""
        try:
            logger.info(f"正在连接表: {self.source_table_name} -> {self.target_table_name}")

            # 处理空键列情况
            source_keys = self.source_key_columns.split(',') if self.source_key_columns else []
            target_keys = self.target_key_columns.split(',') if self.target_key_columns else []

            # 清理空字符串
            source_keys = [k.strip() for k in source_keys if k.strip()]
            target_keys = [k.strip() for k in target_keys if k.strip()]

            if not source_keys and not target_keys:
                self.error = "源表和目标表都无有效主键"
                logger.error(self.error)
                return False

            # 安全解析URL的函数（支持特殊字符密码）
            def parse_and_clean_url(url, db_type):
                """安全解析URL并移除schema部分"""
                import re
                # 使用正则表达式安全解析
                pattern = r'^(\w+)://([^:]+):([^@]+)@([^:/]+):(\d+)/([^/]+)/(.*)$'
                match = re.match(pattern, url)

                if match:
                    # 提取组件
                    db_type = match.group(1)
                    username = match.group(2)
                    password = match.group(3)
                    host = match.group(4)
                    port = int(match.group(5))
                    database = match.group(6)
                    schema = match.group(7)

                    # Oracle特殊处理：schema大写
                    if db_type == 'oracle' and schema:
                        schema = schema.upper()

                    # 重建不带schema的URL
                    clean_url = f"{db_type}://{username}:{password}@{host}:{port}/{database}"
                    return clean_url, schema
                else:
                    # 备用方案：使用urlparse
                    parsed = urlparse(url)
                    username = parsed.username
                    password = parsed.password
                    host = parsed.hostname
                    port = parsed.port

                    # 解析路径部分
                    path_parts = parsed.path.strip('/').split('/')
                    database = path_parts[0] if path_parts else None
                    schema = path_parts[1] if len(path_parts) > 1 else None

                    # Oracle特殊处理：schema大写
                    if db_type == 'oracle' and schema:
                        schema = schema.upper()

                    # 重建不带schema的URL
                    clean_path = f"/{database}" if database else ""
                    clean_url = parsed._replace(path=clean_path).geturl()
                    return clean_url, schema

            # 获取数据库类型（通过URL中的协议）
            src_db_type = self.db1_url.split("://")[0].lower()
            tgt_db_type = self.db2_url.split("://")[0].lower()

            # 解析并清理URL
            src_clean_url, self.src_schema = parse_and_clean_url(self.db1_url, src_db_type)
            logger.debug(f"清理后的源URL: {src_clean_url}")

            tgt_clean_url, self.tgt_schema = parse_and_clean_url(self.db2_url, tgt_db_type)
            logger.debug(f"清理后的目标URL: {tgt_clean_url}")

            # 构造带schema的表名
            def format_table_name(table_name, db_type, schema):
                """格式化表名（添加schema前缀）"""
                if db_type in ['postgresql', 'mssql', 'oracle'] and schema:
                    return f"{schema}.{table_name}"
                return table_name

            source_table = format_table_name(
                self.source_table_name,
                src_db_type,
                self.src_schema
            )
            target_table = format_table_name(
                self.target_table_name,
                tgt_db_type,
                self.tgt_schema
            )

            logger.info(f"源表完整名称: {source_table}, 目标表完整名称: {target_table}")
            logger.info(f"源键: {source_keys}, 目标键: {target_keys}")

            # ====== 专门为 MSSQL 添加 schema 处理 ======
            if src_db_type == 'mssql' and self.src_schema:
                # 在连接字符串中添加 schema 信息
                if ";" in src_clean_url:
                    src_clean_url += f"/{self.src_schema}"
                else:
                    src_clean_url += f"/{self.src_schema}"
                logger.debug(f"为MSSQL源表添加schema: {self.src_schema}")

            if tgt_db_type == 'mssql' and self.tgt_schema:
                # 在连接字符串中添加 schema 信息
                if ";" in tgt_clean_url:
                    tgt_clean_url += f"/{self.tgt_schema}"
                else:
                    tgt_clean_url += f"/{self.tgt_schema}"
                logger.debug(f"为MSSQL目标表添加schema: {self.tgt_schema}")
            # ====== MSSQL 特殊处理结束 ======

            # 连接到数据库表
            self.source = connect_to_table(
                src_clean_url,
                table_name=source_table,
                key_columns=source_keys
            )

            self.target = connect_to_table(
                tgt_clean_url,
                table_name=target_table,
                key_columns=target_keys
            )

            logger.info("表连接成功")
            return True
        except Exception as e:
            self.error = f"连接表失败: {str(e)}"
            logger.error(self.error)
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return False

    def _format_table_name(self, table_name, db_type, schema):
        """格式化表名（添加schema前缀）"""
        if db_type in ['postgresql', 'mssql', 'oracle']:
            if schema:
                return f"{schema}.{table_name}"
        return table_name

    def _generate_diff(self):
        """生成数据差异报告（使用原始大小写）"""
        try:
            logger.info(f"开始对比表: {self.source_table_name} -> {self.target_table_name}")

            # 获取额外的列参数
            extra_cols = tuple(self.extra_columns) if self.extra_columns else None

            # 执行差异检测
            diff = diff_tables(
                self.source,
                self.target,
                extra_columns=extra_cols,
                threaded=True
            )

            for diff_type, _ in diff:
                if diff_type == '-':
                    self.missing_rows += 1
                elif diff_type == '+':
                    self.added_rows += 1

            logger.info(f"表对比完成: {self.source_table_name} -> {self.target_table_name}")
            return True
        except Exception as e:
            self.error = f"对比过程中出错: {str(e)}"
            logger.error(f"对比表 {self.source_table_name} -> {self.target_table_name} 失败: {str(e)}")
            logger.debug(f"错误详情: {traceback.format_exc()}")

            # 尝试记录表结构信息
            try:
                if self.source:
                    logger.debug(f"源表结构: {self.source._schema}")
                if self.target:
                    logger.debug(f"目标表结构: {self.target._schema}")
            except:
                logger.debug("无法获取表结构信息")

            return False


def find_matching_columns(columns, key_list):
    """在表中查找匹配的列（忽略大小写）并返回真实列名"""
    matched_keys = []
    for key in key_list:
        # 在表中查找匹配项（忽略大小写）
        matched = False
        for col in columns:
            if col.lower() == key.lower():
                matched_keys.append(col)  # 使用表真实列名
                matched = True
                break
        if not matched:
            logger.warning(f"键列 '{key}' 在表中无匹配列")
            return None  # 如果任一列不匹配，返回None
    return matched_keys
