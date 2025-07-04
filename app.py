from flask import Flask, render_template, request, redirect, url_for, jsonify, session, copy_current_request_context, flash
from flask_session import Session
import sqlite3
import os
import threading
import logging
import traceback
from datetime import datetime
import socket
import time
import json
import traceback
from compare import DatabaseInspector, TableDiffReporter, SingleTableDiffReporter

app = Flask(__name__)
app.secret_key = os.urandom(24)
app.logger.setLevel(logging.DEBUG)


app.config.update(
    SESSION_TYPE='filesystem',
    SESSION_FILE_DIR='/tmp/flask_session',
    SESSION_COOKIE_SECURE=False,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=3600,
    TEMPLATES_AUTO_RELOAD=True
)
Session(app)

REPORTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), 'reports'))
os.makedirs(REPORTS_DIR, exist_ok=True)
MAX_RETRIES = 20
POLL_INTERVAL = 3
COMPARE_TIMEOUT = 300  # 5分钟超时

# 新增：数据库URL构建函数
def build_db_url(db_type, username, password, host, port, database, schema=None):
    """构建带URL编码的数据库连接字符串"""
    
    # 处理Oracle和SQL Server/PostgreSQL的schema逻辑
    if schema is not None:
        if db_type == 'oracle':
            schema = schema.upper()
        elif db_type in ['mssql', 'postgresql'] and not schema:
            schema = 'dbo' if db_type == 'mssql' else 'public'
    
    # 构建基本URL
    if db_type in ['mssql', 'postgresql', 'oracle']:
        return f"{db_type}://{username}:{password}@{host}:{port}/{database}/{schema}" if schema else \
               f"{db_type}://{username}:{password}@{host}:{port}/{database}"
    else:
        return f"{db_type}://{username}:{password}@{host}:{port}/{database}"

def init_db():
    with sqlite3.connect('datasources.db') as conn:
        conn.execute('''CREATE TABLE IF NOT EXISTS datasources (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL UNIQUE,
                        db_type TEXT NOT NULL,
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        database TEXT NOT NULL,
                        username TEXT NOT NULL,
                        password TEXT NOT NULL,
                        schema TEXT DEFAULT '',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

init_db()

def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return '0.0.0.0'

@app.context_processor
def inject_now():
    return {'now': datetime.utcnow()}

@app.route('/')
def index():
    try:
        with sqlite3.connect('datasources.db') as conn:
            conn.row_factory = sqlite3.Row
            datasources = conn.execute("SELECT * FROM datasources ORDER BY created_at DESC").fetchall()
        return render_template('index.html', datasources=[dict(row) for row in datasources])
    except Exception as e:
        app.logger.error(f"首页加载失败: {traceback.format_exc()}")
        return render_template('error.html', 
                            message="加载数据源列表失败",
                            error=str(e)), 500

@app.route('/datasource/add', methods=['GET', 'POST'])
def add_datasource():
    error = None
    if request.method == 'POST':
        try:
            required_fields = ['name', 'db_type', 'host', 'port', 'database', 'username', 'password']
            if not all(field in request.form for field in required_fields):
                raise ValueError("缺少必要字段")
            
            schema = request.form.get('schema', '')
            db_type = request.form['db_type']
            
            if db_type == 'oracle' and not schema:
                schema = request.form['username'].upper()
            elif db_type == 'oracle' and schema:
                schema = schema.upper()
            
            if db_type in ['mssql', 'postgresql'] and not schema:
                schema = 'dbo' if db_type == 'mssql' else 'public'

            with sqlite3.connect('datasources.db') as conn:
                conn.execute('''INSERT INTO datasources 
                            (name, db_type, host, port, database, username, password, schema)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                            (request.form['name'], 
                             db_type,
                             request.form['host'],
                             int(request.form['port']),
                             request.form['database'],
                             request.form['username'],
                             request.form['password'],
                             schema))
            flash('数据源添加成功', 'success')
            return redirect(url_for('index'))
        except sqlite3.IntegrityError:
            error = "数据源名称已存在"
        except ValueError as ve:
            error = f"参数错误: {str(ve)}"
        except Exception as e:
            error = f"添加失败: {str(e)}"
            app.logger.error(f"添加数据源失败: {traceback.format_exc()}")
    return render_template('datasource_form.html', error=error)

@app.route('/datasource/edit/<int:id>', methods=['GET', 'POST'])
def edit_datasource(id):
    try:
        with sqlite3.connect('datasources.db') as conn:
            conn.row_factory = sqlite3.Row
            ds = conn.execute("SELECT * FROM datasources WHERE id=?", (id,)).fetchone()
        
        if not ds:
            flash('数据源不存在', 'danger')
            return redirect(url_for('index'))
        
        error = None
        if request.method == 'POST':
            try:
                required_fields = ['name', 'db_type', 'host', 'port', 'database', 'username', 'password']
                if not all(field in request.form for field in required_fields):
                    raise ValueError("缺少必要字段")
                
                schema = request.form.get('schema', '')
                db_type = request.form['db_type']
                
                if db_type == 'oracle' and not schema:
                    schema = request.form['username'].upper()
                elif db_type == 'oracle' and schema:
                    schema = schema.upper()
                
                if db_type in ['mssql', 'postgresql'] and not schema:
                    schema = 'dbo' if db_type == 'mssql' else 'public'

                with sqlite3.connect('datasources.db') as conn:
                    conn.execute('''UPDATE datasources SET
                                name=?, db_type=?, host=?, port=?, database=?, username=?, password=?, schema=?
                                WHERE id=?''',
                                (request.form['name'],
                                 db_type,
                                 request.form['host'],
                                 int(request.form['port']),
                                 request.form['database'],
                                 request.form['username'],
                                 request.form['password'],
                                 schema,
                                 id))
                flash('数据源更新成功', 'success')
                return redirect(url_for('index'))
            except Exception as e:
                error = f"更新失败: {str(e)}"
                app.logger.error(f"编辑数据源失败: {traceback.format_exc()}")
        
        return render_template('datasource_form.html', 
                            datasource=dict(ds), 
                            error=error)
    
    except Exception as e:
        app.logger.error(f"编辑数据源页面加载失败: {traceback.format_exc()}")
        return render_template('error.html',
                            message="加载编辑页面失败",
                            error=str(e)), 500

@app.route('/datasource/delete/<int:id>', methods=['POST'])
def delete_datasource(id):
    try:
        with sqlite3.connect('datasources.db') as conn:
            cur = conn.execute("DELETE FROM datasources WHERE id=?", (id,))
            if cur.rowcount == 0:
                flash('数据源不存在', 'warning')
            else:
                flash('数据源删除成功', 'success')
        return redirect(url_for('index'))
    except Exception as e:
        app.logger.error(f"删除数据源失败: {traceback.format_exc()}")
        flash('删除数据源失败', 'danger')
        return redirect(url_for('index'))

@app.route('/test_connection', methods=['POST'])
def test_connection():
    try:
        data = request.get_json()
        if not data:
            raise ValueError("缺少请求数据")
        
        required_fields = ['db_type', 'host', 'port', 'database', 'username', 'password']
        if not all(field in data for field in required_fields):
            raise ValueError("缺少必要字段")
        
        schema = data.get('schema', '')
        db_type = data['db_type']
        
        if db_type == 'oracle' and not schema:
            schema = data['username'].upper()
        elif db_type == 'oracle' and schema:
            schema = schema.upper()
        
        if db_type in ['mssql', 'postgresql'] and not schema:
            schema = 'dbo' if db_type == 'mssql' else 'public'
        
        # 使用统一的URL构建方法
        db_url = build_db_url(
            db_type=db_type,
            username=data['username'],
            password=data['password'],
            host=data['host'],
            port=data['port'],
            database=data['database'],
            schema=schema
        )
        
        inspector = DatabaseInspector(db_url)
        tables = inspector.get_all_tables()
        inspector.close()
        return jsonify({
            'success': True,
            'message': '连接成功',
            'tables_count': len(tables)
        })
    except Exception as e:
        app.logger.error(f"连接测试失败: {traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': f"连接失败: {str(e)}"
        }), 500

@app.route('/compare', endpoint='compare_index')
def compare_index():
    try:
        with sqlite3.connect('datasources.db') as conn:
            conn.row_factory = sqlite3.Row
            datasources = conn.execute("SELECT id, name, db_type FROM datasources").fetchall()
        return render_template('compare.html', datasources=[dict(row) for row in datasources])
    except Exception as e:
        app.logger.error(f"对比页面加载失败: {traceback.format_exc()}")
        return render_template('error.html',
                            message="加载对比页面失败",
                            error=str(e)), 500

@app.route('/get_tables', methods=['POST'])
def get_tables():
    try:
        source_id = request.form.get('source_id')
        if not source_id:
            raise ValueError("缺少source_id参数")

        with sqlite3.connect('datasources.db') as conn:
            conn.row_factory = sqlite3.Row
            ds = conn.execute("SELECT * FROM datasources WHERE id=?", (source_id,)).fetchone()
        
        if not ds:
            return jsonify({'error': '数据源不存在'}), 404
        
        db_type = ds['db_type']
        schema = ds['schema'] or ''
        
        if db_type == 'oracle' and not schema:
            schema = ds['username'].upper()
        elif db_type == 'oracle' and schema:
            schema = schema.upper()
        
        if db_type in ['mssql', 'postgresql'] and not schema:
            schema = 'dbo' if db_type == 'mssql' else 'public'
        
        # 使用统一的URL构建方法
        db_url = build_db_url(
            db_type=db_type,
            username=ds['username'],
            password=ds['password'],
            host=ds['host'],
            port=ds['port'],
            database=ds['database'],
            schema=schema
        )
        
        app.logger.debug(f"尝试连接数据库: {db_url}")
        
        inspector = DatabaseInspector(db_url)
        tables = inspector.get_all_tables()
        inspector.close()
        app.logger.info(f"获取到 {len(tables)} 个表")
        return jsonify({'tables': tables})
    except Exception as e:
        app.logger.error(f"获取表列表失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_table_columns', methods=['POST'])
def get_table_columns():
    try:
        source_id = request.form.get('source_id')
        table_name = request.form.get('table_name')
        if not source_id or not table_name:
            raise ValueError("缺少必要参数")

        with sqlite3.connect('datasources.db') as conn:
            conn.row_factory = sqlite3.Row
            ds = conn.execute("SELECT * FROM datasources WHERE id=?", (source_id,)).fetchone()
        
        if not ds:
            return jsonify({'error': '数据源不存在'}), 404
        
        db_type = ds['db_type']
        schema = ds['schema'] or ''
        
        if db_type == 'oracle' and not schema:
            schema = ds['username'].upper()
        elif db_type == 'oracle' and schema:
            schema = schema.upper()
        
        if db_type in ['mssql', 'postgresql'] and not schema:
            schema = 'dbo' if db_type == 'mssql' else 'public'
        
        # 使用统一的URL构建方法
        db_url = build_db_url(
            db_type=db_type,
            username=ds['username'],
            password=ds['password'],
            host=ds['host'],
            port=ds['port'],
            database=ds['database'],
            schema=schema
        )
            
        inspector = DatabaseInspector(db_url)
        columns = inspector.get_table_columns(table_name)
        keys, key_type = inspector.get_table_keys(table_name)
        inspector.close()
        return jsonify({
            'columns': columns,
            'keys': keys or '',
            'key_type': key_type or ''
        })
    except Exception as e:
        app.logger.error(f"获取表列信息失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/start_compare', methods=['POST'])
def start_compare():
    try:
        required_fields = ['source_id', 'target_id', 'compare_type']
        if not all(field in request.form for field in required_fields):
            raise ValueError("缺少必要参数")

        with sqlite3.connect('datasources.db') as conn:
            conn.row_factory = sqlite3.Row
            source_ds = conn.execute("SELECT * FROM datasources WHERE id=?", (request.form['source_id'],)).fetchone()
            target_ds = conn.execute("SELECT * FROM datasources WHERE id=?", (request.form['target_id'],)).fetchone()
        
        if not source_ds or not target_ds:
            return jsonify({'error': '数据源不存在'}), 404
        
        # 处理源数据库URL
        source_db_type = source_ds['db_type']
        source_schema = source_ds['schema'] or ''
        if source_db_type == 'oracle':
            if not source_schema:
                source_schema = source_ds['username'].upper()
            else:
                source_schema = source_schema.upper()
        elif source_db_type in ['mssql', 'postgresql'] and not source_schema:
            source_schema = 'dbo' if source_db_type == 'mssql' else 'public'
        
        # 处理目标数据库URL
        target_db_type = target_ds['db_type']
        target_schema = target_ds['schema'] or ''
        if target_db_type == 'oracle':
            if not target_schema:
                target_schema = target_ds['username'].upper()
            else:
                target_schema = target_schema.upper()
        elif target_db_type in ['mssql', 'postgresql'] and not target_schema:
            target_schema = 'dbo' if target_db_type == 'mssql' else 'public'
        
        # 使用统一的URL构建方法（源数据库）
        source_url = build_db_url(
            db_type=source_db_type,
            username=source_ds['username'],
            password=source_ds['password'],
            host=source_ds['host'],
            port=source_ds['port'],
            database=source_ds['database'],
            schema=source_schema
        )
        
        # 使用统一的URL构建方法（目标数据库）
        target_url = build_db_url(
            db_type=target_db_type,
            username=target_ds['username'],
            password=target_ds['password'],
            host=target_ds['host'],
            port=target_ds['port'],
            database=target_ds['database'],
            schema=target_schema
        )
        
        report_id = f"report_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        report_path = os.path.join(REPORTS_DIR, f"{report_id}.html")
        realtime_report_path = os.path.join(REPORTS_DIR, f"{report_id}_realtime.json")

        compare_params = {
            'db1_url': source_url,
            'db2_url': target_url,
            'compare_type': request.form['compare_type'],
            'source_db': source_ds['name'],
            'target_db': target_ds['name'],
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'report_id': report_id,
            'report_path': os.path.abspath(report_path),
            'start_time': time.time(),  # 记录开始时间
            'realtime_report_path': realtime_report_path  # 添加实时报告路径
        }
        print(compare_params)

        # 立即生成初始报告文件
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(generate_processing_report(compare_params))

        if compare_params['compare_type'] == 'single':
            required_table_fields = ['source_table', 'target_table']
            if not all(field in request.form for field in required_table_fields):
                raise ValueError("缺少表名参数")
            
            compare_params.update({
                'source_table_name': request.form['source_table'],
                'target_table_name': request.form['target_table'],
                'source_key_columns': request.form.get('source_keys', ''),
                'target_key_columns': request.form.get('target_keys', '')
            })
        elif compare_params['compare_type'] in ('multi', 'full'):
            table_pairs = []
            key_mappings = {}
            i = 0
            while f'tables[{i}][source_table]' in request.form:
                source_table = request.form[f'tables[{i}][source_table]']
                target_table = request.form[f'tables[{i}][target_table]']
                source_keys = request.form.get(f'tables[{i}][source_keys]', '')
                target_keys = request.form.get(f'tables[{i}][target_keys]', '')
                table_pairs.append((source_table, target_table))
                if source_keys or target_keys:
                    key_mappings[source_table] = (source_keys, target_keys)
                i += 1
            compare_params.update({
                'table_pairs': table_pairs,
                'key_mappings': key_mappings
            })
        
        # 初始化session中的报告状态
        with app.app_context():
            session.setdefault('reports', {})[report_id] = {
                'path': compare_params['report_path'],
                'realtime_path': realtime_report_path,  # 存储实时报告路径
                'source': compare_params['source_db'],
                'target': compare_params['target_db'],
                'timestamp': compare_params['timestamp'],
                'status': 'processing',
                'progress': 0,
                'start_time': compare_params['start_time'],
                'created_at': datetime.now().isoformat()
            }
            session.modified = True
            session.permanent = True
            app.logger.debug(f"初始化报告session: {report_id}")
        
        @copy_current_request_context
        def run_comparison_with_context(params):
            run_comparison(params)
        
        threading.Thread(target=run_comparison_with_context, args=(compare_params,)).start()
        
        return jsonify({
            'status': 'started', 
            'message': '对比任务已开始',
            'report_id': report_id,
            'report_url': url_for('view_report', report_id=report_id),
            'realtime_report_url': url_for('view_realtime_report', report_id=report_id),  # 添加实时报告URL
            'max_retries': MAX_RETRIES,
            'poll_interval': POLL_INTERVAL,
            'timeout': COMPARE_TIMEOUT
        })
    except Exception as e:
        app.logger.error(f"启动对比任务失败: {traceback.format_exc()}")
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

def run_comparison(params):
    try:
        # 记录开始时间
        start_time = time.time()
        
        # 更新进度
        with app.app_context():
            session['reports'][params['report_id']]['progress'] = 10
            session.modified = True
            session.permanent = True
            app.logger.info(f"更新进度 10%: {params['report_id']}")
        
        # 创建比较器实例
        if params['compare_type'] == 'single':
            reporter = SingleTableDiffReporter(
                db1_url=params['db1_url'],
                db2_url=params['db2_url'],
                source_table_name=params['source_table_name'],
                target_table_name=params['target_table_name'],
                source_key_columns=params['source_key_columns'],
                target_key_columns=params['target_key_columns']
            )
            result = reporter.execute_comparison()
            html_content = generate_single_report(result, params)
        else:
            # 准备基础参数
            reporter_kwargs = {
                'db1_url': params['db1_url'],
                'db2_url': params['db2_url'],
                'table_pairs': params.get('table_pairs', []),
                'key_mappings': params.get('key_mappings', {})
            }
            
            # 仅在全库对比时添加实时报告路径
            if params['compare_type'] == 'full':
                reporter_kwargs['realtime_report_path'] = params.get('realtime_report_path')
            
            reporter = TableDiffReporter(**reporter_kwargs)
            results, html_path = reporter.run_report()
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
        
        # 检查是否超时
        elapsed = time.time() - start_time
        if elapsed > COMPARE_TIMEOUT:
            # 生成超时报告
            html_content = generate_timeout_report(params, elapsed)
            app.logger.warning(f"对比任务 {params['report_id']} 超时: {elapsed:.2f}秒")
        
        # 写入报告文件
        with open(params['report_path'], 'w', encoding='utf-8') as f:
            f.write(html_content)
            app.logger.info(f"报告文件已保存: {params['report_path']}")
        
        # 更新session状态
        final_status = 'timeout' if elapsed > COMPARE_TIMEOUT else 'completed'
        with app.app_context():
            session['reports'][params['report_id']].update({
                'status': final_status,
                'progress': 100,
                'finished_at': datetime.now().isoformat(),
                'elapsed_time': f"{elapsed:.2f}秒"
            })
            session.modified = True
            session.permanent = True
            app.logger.info(f"更新session状态为{final_status}: {params['report_id']}")
    
    except Exception as e:
        error_msg = f"对比失败: {str(e)}"
        app.logger.error(error_msg)
        app.logger.error(traceback.format_exc())
        
        # 生成错误报告
        with open(params['report_path'], 'w', encoding='utf-8') as f:
            f.write(generate_error_report(params, error_msg))
        
        with app.app_context():
            session['reports'][params['report_id']].update({
                'status': 'error',
                'error': error_msg,
                'progress': 100,
                'finished_at': datetime.now().isoformat()
            })
            session.modified = True
            session.permanent = True
            app.logger.info("错误状态已更新到session")

def generate_processing_report(params):
    """生成处理中状态的报告内容"""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>数据库对比处理中</title>
    <style>
        body {{ font-family: 'Microsoft YaHei', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; padding: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 800px; margin: 40px auto; background: white; 
                     padding: 30px; border-radius: 8px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #2c3e50; text-align: center; margin-bottom: 30px; }}
        .progress-container {{ margin: 30px 0; }}
        .progress {{ height: 25px; border-radius: 5px; overflow: hidden; }}
        .progress-bar {{ background-color: #3498db; }}
        .info-card {{ background-color: #f1f8ff; border-left: 4px solid #3498db; 
                     padding: 15px; margin: 20px 0; border-radius: 0 4px 4px 0; }}
        .status {{ text-align: center; font-size: 20px; font-weight: bold; color: #3498db; }}
        .spinner {{ display: inline-block; width: 1em; height: 1em; 
                   border: 3px solid rgba(0,0,0,.1); 
                   border-left-color: #3498db; 
                   border-radius: 50%; 
                   animation: spin 1s linear infinite; 
                   margin-right: 10px; }}
        @keyframes spin {{
            to {{ transform: rotate(360deg); }}
        }}
        .btn-container {{ text-align: center; margin-top: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>数据库对比处理中</h1>
        
        <div class="info-card">
            <p><strong>源数据库:</strong> {params['source_db']}</p>
            <p><strong>目标数据库:</strong> {params['target_db']}</p>
            <p><strong>开始时间:</strong> {params['timestamp']}</p>
            <p><strong>对比类型:</strong> {params['compare_type']}</p>
        </div>
        
        <div class="status">
            <div class="spinner"></div> 正在对比，请稍候...
        </div>
        
        <div class="progress-container">
            <div class="progress">
                <div class="progress-bar" role="progressbar" style="width: 10%;" 
                     aria-valuenow="10" aria-valuemin="0" aria-valuemax="100">10%</div>
            </div>
        </div>
        
        <p class="text-center text-muted">
            此页面将自动刷新显示最终结果，如果长时间未完成，请检查数据库性能或联系管理员。
        </p>
        
        <div class="btn-container">
            <a href="{url_for('view_realtime_report', report_id=params['report_id'])}" class="btn btn-primary">
                查看实时进度报告
            </a>
        </div>
    </div>
    
    <script>
        // 自动刷新页面以更新进度
        setTimeout(function() {{
            window.location.reload();
        }}, 10000);  // 每10秒刷新一次
    </script>
</body>
</html>"""

def generate_timeout_report(params, elapsed):
    """生成超时报告内容"""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>数据库对比超时</title>
    <style>
        body {{ font-family: 'Microsoft YaHei', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; padding: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 800px; margin: 40px auto; background: white; 
                     padding: 30px; border-radius: 8px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #e74c3c; text-align: center; margin-bottom: 30px; }}
        .alert {{ background-color: #fdecea; border-left: 4px solid #e74c3c; 
                 padding: 15px; margin: 20px 0; border-radius: 0 4px 4px 0; }}
        .info-card {{ background-color: #f9f9f9; border-left: 4px solid #95a5a6; 
                     padding: 15px; margin: 20px 0; border-radius: 0 4px 4px 0; }}
        .icon {{ font-size: 60px; text-align: center; color: #e74c3c; margin: 20px 0; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="icon">⏱️</div>
        <h1>数据库对比超时</h1>
        
        <div class="alert">
            <p><strong>对比操作已超过最大允许时间！</strong></p>
            <p>对比任务已运行 <strong>{elapsed:.1f} 秒</strong>，超过系统设置的超时时间 ({COMPARE_TIMEOUT} 秒)。</p>
            <p>可能原因：</p>
            <ul>
                <li>数据库表数据量过大</li>
                <li>数据库服务器性能不足</li>
                <li>网络连接问题</li>
                <li>复杂表结构导致对比耗时增加</li>
            </ul>
            <p>建议：</p>
            <ul>
                <li>减少对比数据量或选择部分表对比</li>
                <li>优化数据库服务器性能</li>
                <li>检查网络连接状况</li>
            </ul>
        </div>
        
        <div class="info-card">
            <p><strong>源数据库:</strong> {params['source_db']}</p>
            <p><strong>目标数据库:</strong> {params['target_db']}</p>
            <p><strong>开始时间:</strong> {params['timestamp']}</p>
            <p><strong>对比类型:</strong> {params['compare_type']}</p>
            <p><strong>运行时间:</strong> {elapsed:.1f} 秒</p>
        </div>
        
        <div class="text-center">
            <a href="/compare" class="btn btn-primary">返回对比页面</a>
            <a href="/reports" class="btn btn-secondary">查看其他报告</a>
        </div>
    </div>
</body>
</html>"""

def generate_single_report(result, params):
    """生成单表对比报告（与多表报告列保持一致）"""
    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>单表对比报告</title>
    <style>
        body {{ 
            font-family: 'Microsoft YaHei', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
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
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
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
        .status-cell {{
            font-weight: bold;
            text-align: center;
            min-width: 100px;
        }}
        .summary {{
            background-color: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 5px;
            padding: 15px;
            margin-bottom: 20px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>单表对比报告</h1>
        <p>源数据库: {params['source_db']}</p>
        <p>目标数据库: {params['target_db']}</p>
        <p>对比时间: {params['timestamp']}</p>
    </div>

    <div class="summary">
        <h2>对比概览</h2>
        <table>
            <tr>
                <td width="120px"><strong>源表名:</strong></td>
                <td>{params['source_table_name']}</td>
            </tr>
            <tr>
                <td><strong>目标表名:</strong></td>
                <td>{params['target_table_name']}</td>
            </tr>
            <tr>
                <td><strong>对比结果:</strong></td>
                <td>{result.get('状态', '')}</td>
            </tr>
        </table>
    </div>

    <h2>详细比对结果</h2>
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
            <tr>
                <td>{params['source_table_name']}</td>
                <td>{params['target_table_name']}</td>
                <td class="status-cell">{result.get('状态', '')}</td>
                <td>{result.get('键情况', '')}</td>
                <td>{result.get('差异详情', '')}</td>
                <td>{result.get('缺失行', 0)}</td>
                <td>{result.get('新增行', 0)}</td>
                <td>{result.get('总差异数', 0)}</td>
            </tr>
        </tbody>
    </table>
</body>
</html>"""


def generate_error_report(params, error_msg):
    """生成错误报告内容"""
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>数据库对比错误</title>
    <style>
        body {{ font-family: 'Microsoft YaHei', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                margin: 0; padding: 20px; background-color: #f8f9fa; }}
        .container {{ max-width: 800px; margin: 40px auto; background: white; 
                     padding: 30px; border-radius: 8px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
        h1 {{ color: #e74c3c; text-align: center; margin-bottom: 30px; }}
        .alert {{ background-color: #fdecea; border-left: 4px solid #e74c3c; 
                 padding: 15px; margin: 20px 0; border-radius: 0 4px 4px 0; }}
        .info-card {{ background-color: #f9f9f9; border-left: 4px solid #95a5a6; 
                     padding: 15px; margin: 20px 0; border-radius: 0 4px 4px 0; }}
        pre {{ background-color: #2c3e50; color: #ecf0f1; padding: 15px; 
              border-radius: 4px; overflow-x: auto; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>数据库对比错误</h1>
        
        <div class="alert">
            <p><strong>对比过程中发生错误：</strong></p>
            <p>{error_msg}</p>
        </div>
        
        <div class="info-card">
            <p><strong>源数据库:</strong> {params['source_db']}</p>
            <p><strong>目标数据库:</strong> {params['target_db']}</p>
            <p><strong>开始时间:</strong> {params['timestamp']}</p>
            <p><strong>对比类型:</strong> {params['compare_type']}</p>
        </div>
        
        <div class="text-center">
            <a href="/compare" class="btn btn-primary">返回对比页面</a>
            <a href="/reports" class="btn btn-secondary">查看其他报告</a>
        </div>
    </div>
</body>
</html>"""

@app.route('/reports')
def list_reports():
    try:
        reports = session.get('reports', {})
        
        valid_reports = {}
        for report_id, report_info in reports.items():
            file_path = report_info.get('path')
            if file_path and os.path.exists(file_path):
                valid_reports[report_id] = report_info
            else:
                app.logger.warning(f"自动移除无效报告: {report_id}")
        
        if len(valid_reports) != len(reports):
            session['reports'] = valid_reports
            session.modified = True
        
        return render_template('report_list.html', reports=valid_reports)
    except Exception as e:
        app.logger.error(f"报告列表加载失败: {traceback.format_exc()}")
        return render_template('error.html',
                            message="加载报告列表失败",
                            error=str(e)), 500

@app.route('/report')
def redirect_to_reports():
    return redirect(url_for('list_reports'))

@app.route('/report/<report_id>')
def view_report(report_id):
    try:
        report_info = session.get('reports', {}).get(report_id)
        if not report_info:
            flash('报告不存在或已被删除', 'warning')
            return redirect(url_for('list_reports'))
        
        abs_report_path = report_info.get('path', '')
        if not os.path.exists(abs_report_path):
            flash('报告文件不存在', 'danger')
            return redirect(url_for('list_reports'))
        
        with open(abs_report_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return render_template('report_viewer.html', 
                            content=content,
                            report=report_info)
    except Exception as e:
        app.logger.error(f"查看报告失败: {traceback.format_exc()}")
        return render_template('error.html',
                            message="加载报告失败",
                            error=str(e)), 500

@app.route('/report/<report_id>/realtime')
def view_realtime_report(report_id):
    """查看实时报告页面"""
    try:
        report_info = session.get('reports', {}).get(report_id)
        if not report_info:
            flash('报告不存在或已被删除', 'warning')
            return redirect(url_for('list_reports'))
        
        realtime_path = report_info.get('realtime_path', '')
        if not realtime_path or not os.path.exists(realtime_path):
            flash('实时报告尚未生成', 'info')
            return redirect(url_for('view_report', report_id=report_id))
        
        with open(realtime_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        # 计算进度百分比
        progress_parts = report_data.get('progress', '0/0').split('/')
        completed = int(progress_parts[0]) if len(progress_parts) > 0 else 0
        total = int(progress_parts[1]) if len(progress_parts) > 1 else 0
        progress_percent = round((completed / total) * 100) if total > 0 else 0
        
        return render_template('realtime_report.html',
                            report_id=report_id,
                            report=report_data,
                            progress_percent=progress_percent,
                            now=datetime.now())
    except Exception as e:
        app.logger.error(f"加载实时报告失败: {traceback.format_exc()}")
        return render_template('error.html',
                            message="加载实时报告失败",
                            error=str(e)), 500

@app.route('/report/<report_id>/status')
def report_status(report_id):
    try:
        reports = session.copy().get('reports', {})
        report_info = reports.get(report_id, {})
        
        abs_path = report_info.get('path', '')
        file_exists = os.path.exists(abs_path) if abs_path else False
        
        if file_exists and report_info.get('status') == 'processing':
            app.logger.warning(f"自动修正状态: {report_id}")
            with app.app_context():
                session['reports'][report_id]['status'] = 'completed'
                session['reports'][report_id]['finished_at'] = datetime.now().isoformat()
                session.modified = True
                session.permanent = True
            report_info = session.copy().get('reports', {}).get(report_id, {})
        
        # 添加实时报告状态
        realtime_path = report_info.get('realtime_path', '')
        realtime_exists = os.path.exists(realtime_path) if realtime_path else False
        realtime_status = "unknown"
        realtime_progress = "0/0"
        
        if realtime_exists:
            try:
                with open(realtime_path, 'r', encoding='utf-8') as f:
                    realtime_data = json.load(f)
                realtime_status = realtime_data.get('status', 'unknown')
                realtime_progress = realtime_data.get('progress', '0/0')
            except:
                pass
        
        response = {
            'status': report_info.get('status', 'pending'),
            'message': report_info.get('error', ''),
            'progress': report_info.get('progress', 0),
            'report_exists': file_exists,
            'path': abs_path,
            'server_time': datetime.now().isoformat(),
            'details': {
                'file_size': os.path.getsize(abs_path) if file_exists else 0,
                'last_modified': os.path.getmtime(abs_path) if file_exists else 0
            },
            'realtime': {
                'exists': realtime_exists,
                'status': realtime_status,
                'progress': realtime_progress
            }
        }
        return jsonify(response)
    except Exception as e:
        app.logger.error(f"状态检查失败: {traceback.format_exc()}")
        return jsonify({
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/report/<report_id>/realtime_data')
def get_realtime_report(report_id):
    try:
        report_info = session.get('reports', {}).get(report_id)
        if not report_info:
            return jsonify({'error': '报告不存在或已被删除'}), 404
            
        realtime_path = report_info.get('realtime_path', '')
        if not realtime_path or not os.path.exists(realtime_path):
            return jsonify({'error': '实时报告不存在或尚未生成'}), 404
            
        with open(realtime_path, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
            
        return jsonify(report_data)
        
    except Exception as e:
        app.logger.error(f"获取实时报告失败: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500

@app.route('/report/delete/<report_id>', methods=['POST'])
def delete_report(report_id):
    try:
        reports = session.get('reports', {})
        report_info = reports.pop(report_id, None)
        
        if not report_info:
            return jsonify({'success': False, 'error': '报告不存在'}), 404
        
        # 删除主报告文件
        file_path = report_info.get('path')
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                app.logger.info(f"已删除报告文件: {file_path}")
            except Exception as e:
                app.logger.error(f"主报告文件删除失败: {str(e)}")
        
        # 删除实时报告文件
        realtime_path = report_info.get('realtime_path')
        if realtime_path and os.path.exists(realtime_path):
            try:
                os.remove(realtime_path)
                app.logger.info(f"已删除实时报告文件: {realtime_path}")
            except Exception as e:
                app.logger.error(f"实时报告文件删除失败: {str(e)}")
        
        session['reports'] = reports
        session.modified = True
        return jsonify({'success': True})
    except Exception as e:
        app.logger.error(f"删除报告异常: {traceback.format_exc()}")
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@app.route('/api/compare', methods=['POST'])
def api_compare():
    """
    数据库表对比API
    参数:
    - compare_type: 对比类型 (目前仅支持'single')
    - db1_url: 源数据库连接URL
    - db2_url: 目标数据库连接URL
    - source_table_name: 源表名
    - target_table_name: 目标表名
    - source_key_columns: 源表键列 (可选，逗号分隔)
    - target_key_columns: 目标表键列 (可选，逗号分隔)
    
    返回:
    {
        "source_table": ...,
        "target_table": ...,
        "status": ...,
        "key_info": ...,
        "diff_detail": ...,
        "missing_rows": ...,
        "added_rows": ...,
        "total_diffs": ...,
        "error_detail": ... (如果有错误)
    }
    """
    try:
        # 获取请求数据
        data = request.get_json()
        
        # 验证必要参数
        required_fields = ['compare_type', 'db1_url', 'db2_url', 
                          'source_table_name', 'target_table_name']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    "error": f"Missing required parameter: {field}",
                    "required_fields": required_fields
                }), 400
        
        # 目前只支持单表对比
        if data['compare_type'] != 'single':
            return jsonify({
                "error": "Currently only 'single' comparison type is supported",
                "supported_types": ["single"]
            }), 400
        
        # 获取可选参数
        source_key_columns = data.get('source_key_columns', '')
        target_key_columns = data.get('target_key_columns', '')
        
        # 创建对比器
        reporter = SingleTableDiffReporter(
            db1_url=data['db1_url'],
            db2_url=data['db2_url'],
            source_table_name=data['source_table_name'],
            target_table_name=data['target_table_name'],
            source_key_columns=source_key_columns,
            target_key_columns=target_key_columns
        )
        
        # 执行对比
        result = reporter.execute_comparison()
        
        # 返回标准化的结果（使用英文键名）
        return jsonify({
            "source_table": result.get("源库表名", data['source_table_name']),
            "target_table": result.get("目标库表名", data['target_table_name']),
            "status": result.get("状态", "Unknown status"),
            "key_info": result.get("键情况", ""),
            "diff_detail": result.get("差异详情", ""),
            "missing_rows": result.get("缺失行", 0),
            "added_rows": result.get("新增行", 0),
            "total_diffs": result.get("总差异数", 0),
            "error_detail": result.get("错误详情", "")
        })
        
    except Exception as e:
        # 记录详细错误信息
        error_trace = traceback.format_exc()
        app.logger.error(f"API comparison failed: {str(e)}\n{error_trace}")
        
        # 返回错误响应
        return jsonify({
            "error": "Internal server error",
            "message": str(e),
            "traceback": error_trace
        }), 500

if __name__ == '__main__':
    host = get_local_ip()
    port = 5000
    
    os.makedirs('/tmp/flask_session', exist_ok=True)
    os.makedirs(REPORTS_DIR, exist_ok=True)
    
    try:
        os.chmod('/tmp/flask_session', 0o777)
        os.chmod(REPORTS_DIR, 0o777)
    except Exception as e:
        app.logger.error(f"设置权限失败: {str(e)}")
    
    app.run(
        host=host,
        port=port,
        debug=True,
        threaded=True,
        use_reloader=False
    )
    app.logger.info(f"服务已启动: http://{host}:{port}")
