import os
import json
import pyodbc
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, redirect, url_for
from cryptography.fernet import Fernet
import base64
import logging

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this'

# Configuration
CONFIG_FILE = 'servers_config.json'
KEY_FILE = 'encryption.key'

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.servers = []
        self.encryption_key = self.get_or_create_encryption_key()
        self.fernet = Fernet(self.encryption_key)
        self.load_servers()
        
    def get_or_create_encryption_key(self):
        """Create or load encryption key for storing passwords"""
        if os.path.exists(KEY_FILE):
            with open(KEY_FILE, 'rb') as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(KEY_FILE, 'wb') as f:
                f.write(key)
            return key
    
    def encrypt_password(self, password):
        """Encrypt password for storage"""
        if password:
            return self.fernet.encrypt(password.encode()).decode()
        return None
    
    def decrypt_password(self, encrypted_password):
        """Decrypt password for use"""
        if encrypted_password:
            return self.fernet.decrypt(encrypted_password.encode()).decode()
        return None
    
    def load_servers(self):
        """Load server configurations from file"""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    self.servers = json.load(f)
            except Exception as e:
                logger.error(f"Error loading servers config: {e}")
                self.servers = []
        else:
            self.servers = []
            self.save_servers()
    
    def save_servers(self):
        """Save server configurations to file"""
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(self.servers, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving servers config: {e}")
    
    def add_server(self, server_config):
        """Add a new server configuration"""
        # Encrypt password if provided
        if server_config.get('password'):
            server_config['password'] = self.encrypt_password(server_config['password'])
        
        server_config['id'] = max([s.get('id', 0) for s in self.servers], default=0) + 1
        server_config['status'] = 'Unknown'
        server_config['last_checked'] = None
        self.servers.append(server_config)
        self.save_servers()
        return server_config['id']
    
    def update_server(self, server_id, server_config):
        """Update server configuration"""
        server = next((s for s in self.servers if s['id'] == server_id), None)
        if server:
            # Encrypt password if provided and changed
            if server_config.get('password') and server_config['password'] != '***masked***':
                server_config['password'] = self.encrypt_password(server_config['password'])
            elif server_config.get('password') == '***masked***':
                server_config['password'] = server['password']  # Keep existing
            
            server.update(server_config)
            self.save_servers()
            return True
        return False
    
    def delete_server(self, server_id):
        """Delete server configuration"""
        self.servers = [s for s in self.servers if s['id'] != server_id]
        self.save_servers()
        return True
    
    def get_connection_string(self, server):
        """Build connection string based on auth type"""
        try:
            if server['auth_type'] == 'windows':
                if server.get('azure_sql'):
                    return f"Driver={{ODBC Driver 17 for SQL Server}};Server={server['server']};Database={server.get('database', 'master')};Authentication=ActiveDirectoryIntegrated;"
                else:
                    return f"Driver={{ODBC Driver 17 for SQL Server}};Server={server['server']};Database={server.get('database', 'master')};Trusted_Connection=yes;"
            elif server['auth_type'] == 'sql':
                password = self.decrypt_password(server['password']) if server.get('password') else ''
                return f"Driver={{ODBC Driver 17 for SQL Server}};Server={server['server']};Database={server.get('database', 'master')};UID={server['username']};PWD={password};"
            elif server['auth_type'] == 'azure_mfa':
                return f"Driver={{ODBC Driver 17 for SQL Server}};Server={server['server']};Database={server.get('database', 'master')};Authentication=ActiveDirectoryInteractive;"
        except Exception as e:
            logger.error(f"Error building connection string for {server.get('name', 'unknown')}: {e}")
            raise
    
    def test_connection(self, server):
        """Test connection to a server"""
        try:
            conn_str = self.get_connection_string(server)
            conn = pyodbc.connect(conn_str, timeout=10)
            
            # Get basic server info
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    @@SERVERNAME as server_name,
                    @@VERSION as version,
                    GETDATE() as current_time
            """)
            result = cursor.fetchone()
            
            conn.close()
            
            return {
                'success': True,
                'server_name': result[0],
                'version': result[1],
                'current_time': result[2].strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            logger.error(f"Connection test failed for {server.get('name', 'unknown')}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_server_health(self, server):
        """Get comprehensive health information for a server"""
        try:
            conn_str = self.get_connection_string(server)
            conn = pyodbc.connect(conn_str, timeout=15)
            cursor = conn.cursor()
            
            health_data = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'Online',
                'metrics': {}
            }
            
            # Server Info
            cursor.execute("SELECT @@SERVERNAME, @@VERSION")
            result = cursor.fetchone()
            if result:
                health_data['server_name'] = result[0]
                health_data['version'] = result[1][:100] + '...' if len(result[1]) > 100 else result[1]
            
            # CPU Usage (simplified approach)
            try:
                cursor.execute("""
                    SELECT TOP 1 cntr_value 
                    FROM sys.dm_os_performance_counters 
                    WHERE counter_name = 'Processor Queue Length'
                """)
                result = cursor.fetchone()
                health_data['metrics']['cpu_queue_length'] = int(result[0]) if result else 0
            except:
                health_data['metrics']['cpu_queue_length'] = 0
            
            # Memory Usage
            try:
                cursor.execute("""
                    SELECT 
                        (total_physical_memory_kb/1024) as total_memory_mb,
                        (available_physical_memory_kb/1024) as available_memory_mb,
                        ((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb) as memory_usage_pct
                    FROM sys.dm_os_sys_memory
                """)
                result = cursor.fetchone()
                if result:
                    health_data['metrics']['total_memory_mb'] = int(result[0])
                    health_data['metrics']['available_memory_mb'] = int(result[1])
                    health_data['metrics']['memory_usage_pct'] = round(float(result[2]), 2)
            except:
                health_data['metrics']['memory_usage_pct'] = 0
            
            # Database Count and Sizes
            try:
                cursor.execute("""
                    SELECT 
                        COUNT(DISTINCT database_id) as database_count,
                        SUM(CAST(size as bigint) * 8 / 1024) as total_size_mb
                    FROM sys.master_files
                    WHERE type = 0
                """)
                result = cursor.fetchone()
                if result:
                    health_data['metrics']['database_count'] = int(result[0])
                    health_data['metrics']['total_size_mb'] = int(result[1]) if result[1] else 0
            except:
                health_data['metrics']['database_count'] = 0
                health_data['metrics']['total_size_mb'] = 0
            
            # Active Connections
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM sys.dm_exec_sessions 
                    WHERE is_user_process = 1
                """)
                result = cursor.fetchone()
                health_data['metrics']['active_connections'] = int(result[0]) if result else 0
            except:
                health_data['metrics']['active_connections'] = 0
            
            # Blocking Processes
            try:
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM sys.dm_exec_requests 
                    WHERE blocking_session_id > 0
                """)
                result = cursor.fetchone()
                health_data['metrics']['blocked_sessions'] = int(result[0]) if result else 0
            except:
                health_data['metrics']['blocked_sessions'] = 0
            
            conn.close()
            return health_data
            
        except Exception as e:
            logger.error(f"Health check failed for {server.get('name', 'unknown')}: {e}")
            return {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'Error',
                'error': str(e),
                'metrics': {}
            }

# Initialize database manager
db_manager = DatabaseManager()

@app.route('/')
def dashboard():
    """Main dashboard"""
    return render_template('dashboard.html', servers=db_manager.servers)

@app.route('/add_server')
def add_server_form():
    """Show add server form"""
    return render_template('add_server.html')

@app.route('/add_server', methods=['POST'])
def add_server():
    """Add a new server"""
    try:
        server_config = {
            'name': request.form['name'],
            'server': request.form['server'],
            'auth_type': request.form['auth_type'],
            'username': request.form.get('username', ''),
            'password': request.form.get('password', ''),
            'database': request.form.get('database', 'master'),
            'azure_sql': request.form.get('azure_sql') == 'on',
            'environment': request.form.get('environment', 'Production')
        }
        
        server_id = db_manager.add_server(server_config)
        logger.info(f"Added new server: {server_config['name']} (ID: {server_id})")
        return redirect(url_for('dashboard'))
    except Exception as e:
        logger.error(f"Error adding server: {e}")
        return redirect(url_for('add_server_form'))

@app.route('/edit_server/<int:server_id>')
def edit_server_form(server_id):
    """Show edit server form"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return redirect(url_for('dashboard'))
    
    # Mask password for form display
    server_copy = server.copy()
    if server_copy.get('password'):
        server_copy['password'] = '***masked***'
    
    return render_template('edit_server.html', server=server_copy)

@app.route('/edit_server/<int:server_id>', methods=['POST'])
def update_server(server_id):
    """Update server configuration"""
    try:
        server_config = {
            'name': request.form['name'],
            'server': request.form['server'],
            'auth_type': request.form['auth_type'],
            'username': request.form.get('username', ''),
            'password': request.form.get('password', ''),
            'database': request.form.get('database', 'master'),
            'azure_sql': request.form.get('azure_sql') == 'on',
            'environment': request.form.get('environment', 'Production')
        }
        
        if db_manager.update_server(server_id, server_config):
            logger.info(f"Updated server ID: {server_id}")
        return redirect(url_for('dashboard'))
    except Exception as e:
        logger.error(f"Error updating server {server_id}: {e}")
        return redirect(url_for('dashboard'))

@app.route('/delete_server/<int:server_id>', methods=['POST'])
def delete_server(server_id):
    """Delete server configuration"""
    try:
        server = next((s for s in db_manager.servers if s['id'] == server_id), None)
        if server:
            db_manager.delete_server(server_id)
            logger.info(f"Deleted server: {server['name']} (ID: {server_id})")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Error deleting server {server_id}: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/test_connection/<int:server_id>')
def test_connection(server_id):
    """Test connection to a specific server"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'})
    
    result = db_manager.test_connection(server)
    return jsonify(result)

@app.route('/server_health/<int:server_id>')
def server_health(server_id):
    """Get detailed health information for a server"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'})
    
    health_data = db_manager.get_server_health(server)
    
    # Update server with latest health data
    server['last_health'] = health_data
    server['last_checked'] = health_data['timestamp']
    server['status'] = health_data['status']
    db_manager.save_servers()
    
    return jsonify(health_data)

@app.route('/refresh_all')
def refresh_all():
    """Refresh health data for all servers"""
    results = []
    for server in db_manager.servers:
        health_data = db_manager.get_server_health(server)
        server['last_health'] = health_data
        server['last_checked'] = health_data['timestamp']
        server['status'] = health_data['status']
        results.append({
            'id': server['id'],
            'name': server['name'],
            'health': health_data
        })
    
    db_manager.save_servers()
    return jsonify({'success': True, 'results': results})

# Insights API Endpoints
@app.route('/api/insights/missing_indexes/<int:server_id>')
def get_missing_indexes(server_id):
    """Get missing indexes for a server"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'}), 404

    try:
        conn_str = db_manager.get_connection_string(server)
        results = []
        with pyodbc.connect(conn_str, timeout=15) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 20
                    d.statement as table_name,
                    d.equality_columns,
                    d.inequality_columns,
                    d.included_columns,
                    s.avg_total_user_cost * s.avg_user_impact * (s.user_seeks + s.user_scans) as improvement_measure
                FROM sys.dm_db_missing_index_details d
                INNER JOIN sys.dm_db_missing_index_groups g ON d.index_handle = g.index_handle
                INNER JOIN sys.dm_db_missing_index_group_stats s ON g.index_group_handle = s.group_handle
                ORDER BY improvement_measure DESC
            """)
            for row in cursor.fetchall():
                results.append({
                    'table_name': row.table_name.split('.')[-1] if row.table_name else 'Unknown',
                    'equality_columns': row.equality_columns or 'None',
                    'inequality_columns': row.inequality_columns or 'None',
                    'included_columns': row.included_columns or 'None',
                    'improvement_measure': round(float(row.improvement_measure), 2) if row.improvement_measure else 0
                })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        logger.error(f"Error getting missing indexes for server {server_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/insights/index_fragmentation/<int:server_id>')
def get_index_fragmentation(server_id):
    """Get index fragmentation data"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'}), 404

    try:
        conn_str = db_manager.get_connection_string(server)
        data = []
        with pyodbc.connect(conn_str, timeout=20) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 20
                    DB_NAME() as db_name,
                    OBJECT_NAME(ps.object_id) as table_name,
                    i.name as index_name,
                    ps.avg_fragmentation_in_percent,
                    ps.page_count
                FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'SAMPLED') ps
                INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
                WHERE ps.avg_fragmentation_in_percent > 10 
                AND ps.page_count > 1000
                AND i.name IS NOT NULL
                ORDER BY ps.avg_fragmentation_in_percent DESC
            """)
            for row in cursor.fetchall():
                data.append({
                    'db_name': row.db_name,
                    'table_name': row.table_name,
                    'index_name': row.index_name,
                    'fragmentation': round(row.avg_fragmentation_in_percent, 2),
                    'page_count': row.page_count
                })
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        logger.error(f"Error getting index fragmentation for server {server_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/insights/top_cpu_queries/<int:server_id>')
def top_cpu_queries(server_id):
    """Get top CPU consuming queries"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'}), 404

    try:
        conn_str = db_manager.get_connection_string(server)
        data = []
        with pyodbc.connect(conn_str, timeout=15) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 10
                    total_worker_time/1000 AS cpu_ms,
                    execution_count,
                    (total_worker_time/execution_count)/1000 AS avg_cpu_ms,
                    SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 
                        ((CASE qs.statement_end_offset 
                          WHEN -1 THEN DATALENGTH(st.text)
                          ELSE qs.statement_end_offset 
                          END - qs.statement_start_offset)/2) + 1) AS query_text
                FROM sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                WHERE execution_count > 1
                ORDER BY total_worker_time DESC
            """)
            for row in cursor.fetchall():
                query_text = row.query_text.strip() if row.query_text else 'N/A'
                # Limit query text length for display
                if len(query_text) > 200:
                    query_text = query_text[:200] + "..."
                
                data.append({
                    'cpu_ms': int(row.cpu_ms),
                    'execution_count': row.execution_count,
                    'avg_cpu_ms': round(float(row.avg_cpu_ms), 2),
                    'query_text': query_text.replace('\n', ' ').replace('\r', ' ')
                })
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        logger.error(f"Error getting top CPU queries for server {server_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/insights/recovery_model/<int:server_id>')
def get_recovery_model(server_id):
    """Get database recovery models"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'}), 404

    try:
        conn_str = db_manager.get_connection_string(server)
        results = []
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name, recovery_model_desc, state_desc
                FROM sys.databases
                WHERE database_id > 4  -- Exclude system databases
                ORDER BY name
            """)
            for row in cursor.fetchall():
                results.append({
                    'database': row.name,
                    'recovery_model': row.recovery_model_desc,
                    'state': row.state_desc
                })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        logger.error(f"Error getting recovery models for server {server_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/insights/db_sizes/<int:server_id>')
def get_db_sizes(server_id):
    """Get database sizes"""
    server = next((s for s in db_manager.servers if s['id'] == server_id), None)
    if not server:
        return jsonify({'success': False, 'error': 'Server not found'}), 404

    try:
        conn_str = db_manager.get_connection_string(server)
        results = []
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    DB_NAME(database_id) as database_name,
                    SUM(CASE WHEN type = 0 THEN size END) * 8 / 1024 as data_size_mb,
                    SUM(CASE WHEN type = 1 THEN size END) * 8 / 1024 as log_size_mb,
                    SUM(size) * 8 / 1024 as total_size_mb
                FROM sys.master_files
                WHERE database_id > 4  -- Exclude system databases
                GROUP BY database_id
                ORDER BY total_size_mb DESC
            """)
            for row in cursor.fetchall():
                results.append({
                    'database': row.database_name,
                    'data_size_mb': int(row.data_size_mb) if row.data_size_mb else 0,
                    'log_size_mb': int(row.log_size_mb) if row.log_size_mb else 0,
                    'total_size_mb': int(row.total_size_mb)
                })
        return jsonify({'success': True, 'data': results})
    except Exception as e:
        logger.error(f"Error getting database sizes for server {server_id}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/servers')
def get_servers():
    """Get all servers data"""
    return jsonify({'success': True, 'servers': db_manager.servers})

@app.errorhandler(404)
def not_found_error(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return render_template('500.html'), 500

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    
    print("🗃️ Starting DBA Command Center...")
    print("📊 Advanced SQL Server Monitoring & Analytics")
    print("🌐 Access your dashboard at: http://localhost:5000")
    print("⚠️  Press Ctrl+C to stop the server")
    
    app.run(debug=True, host='0.0.0.0', port=5000)