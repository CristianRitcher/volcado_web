import mysql.connector
from mysql.connector import Error
import json
import logging
import argparse
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Set, Any
from pathlib import Path
import time
import hashlib
class MySQLDBConsolidator:

    def __init__(self, source_databases: List[Dict], target_config: Dict, log_file: str = "consolidation_failures.json"):
        self.source_databases = source_databases
        self.target_config = target_config
        self.log_file = log_file
        self.snapshot_file = "consolidation_snapshot.json"
        self.failed_inserts_log = []
        
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('db_consolidation.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_db_connection(self, db_config: Dict) -> mysql.connector.MySQLConnection:
        try:
            conn = mysql.connector.connect(
                host=db_config['host'],
                user=db_config['user'],
                password=db_config['password'],
                database=db_config['database'],
                port=db_config.get('port', 3306),
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci',
                autocommit=False
            )
            return conn
        except Error as e:
            self.logger.error(f"Error conectando a MySQL {db_config['host']}: {e}")
            raise
    
    def get_table_info(self, conn: mysql.connector.MySQLConnection, database: str) -> Dict[str, Dict]:
        cursor = conn.cursor(dictionary=True)
        
        # Obtener todas las tablas
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_TYPE = 'BASE TABLE'
        """, (database,))
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
        
        table_info = {}
        for table in tables:
            # Obtener información de columnas
            cursor.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table))
            columns_info = cursor.fetchall()
            
            all_columns = [col['COLUMN_NAME'] for col in columns_info]
            
            table_info[table] = {
                'all_columns': all_columns,
                'column_types': {col['COLUMN_NAME']: col['DATA_TYPE'] for col in columns_info},
                'column_lengths': {col['COLUMN_NAME']: col['CHARACTER_MAXIMUM_LENGTH'] for col in columns_info}
            }
        
        cursor.close()
        return table_info
    
    def create_target_tables(self, conn: mysql.connector.MySQLConnection):
        cursor = conn.cursor()
        
        # Recopilar esquemas de todas las fuentes
        all_table_schemas = {}
        
        for source_config in self.source_databases:
            source_conn = self.get_db_connection(source_config)
            table_info = self.get_table_info(source_conn, source_config['database'])
            
            for table_name, info in table_info.items():
                if table_name not in all_table_schemas:
                    all_table_schemas[table_name] = {
                        'columns': {},
                        'sources': []
                    }
                
                # Agregar columnas de esta fuente
                for col_name, col_type in info['column_types'].items():
                    if col_name not in all_table_schemas[table_name]['columns']:
                        all_table_schemas[table_name]['columns'][col_name] = col_type
                    # Si existe pero con tipo diferente, usar el más permisivo
                    elif col_type == 'TEXT' or all_table_schemas[table_name]['columns'][col_name] == 'VARCHAR':
                        all_table_schemas[table_name]['columns'][col_name] = 'TEXT'
                
                all_table_schemas[table_name]['sources'].append(source_config['alias'])
            
            source_conn.close()
        
        # Crear tablas en destino
        for table_name, schema in all_table_schemas.items():
            self.logger.info(f"Creando/actualizando tabla consolidada: {table_name}")
            
            # Columnas de trazabilidad
            columns_sql = []
            columns_sql.append("`_consolidation_id` BIGINT AUTO_INCREMENT PRIMARY KEY")
            columns_sql.append("`_source_database` VARCHAR(255) NOT NULL")
            columns_sql.append("`_source_alias` VARCHAR(100) NOT NULL")
            columns_sql.append("`_sync_timestamp` DATETIME DEFAULT CURRENT_TIMESTAMP")
            columns_sql.append("`_record_hash` VARCHAR(64) NOT NULL")
            
            # Agregar columnas originales 
            for col_name, col_type in schema['columns'].items():
                mysql_type = self.convert_to_mysql_type(col_type)
                columns_sql.append(f"`{col_name}` {mysql_type} NULL")
            
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {', '.join(columns_sql)},
                    INDEX `idx_source_alias` (`_source_alias`),
                    INDEX `idx_sync_timestamp` (`_sync_timestamp`),
                    INDEX `idx_record_hash` (`_record_hash`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            
            cursor.execute(create_table_sql)
            conn.commit()
            
            self.logger.info(f"Tabla {table_name} lista para consolidación desde: {', '.join(schema['sources'])}")
        
        cursor.close()
    
    def convert_to_mysql_type(self, original_type: str) -> str:
        type_mapping = {
            'int': 'INT',
            'bigint': 'BIGINT',
            'varchar': 'TEXT',  # Usar TEXT para máxima flexibilidad
            'char': 'TEXT',
            'text': 'TEXT',
            'longtext': 'LONGTEXT',
            'decimal': 'DECIMAL(15,2)',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'datetime': 'DATETIME',
            'date': 'DATE',
            'time': 'TIME',
            'timestamp': 'TIMESTAMP',
            'boolean': 'BOOLEAN',
            'tinyint': 'TINYINT',
            'json': 'JSON'
        }
        
        original_type_lower = original_type.lower()
        for key, value in type_mapping.items():
            if key in original_type_lower:
                return value
        
        # Por defecto, usar TEXT para máxima compatibilidad
        return 'TEXT'
    
    def generate_record_hash(self, record: Dict, source_alias: str) -> str:
        """Genera hash único para detectar registros duplicados"""
        record_string = f"{source_alias}:"
        for key in sorted(record.keys()):
            value = record[key]
            if value is None:
                record_string += f"{key}:NULL|"
            else:
                record_string += f"{key}:{str(value)}|"
        
        return hashlib.sha256(record_string.encode('utf-8')).hexdigest()
    
    def take_snapshot(self):
        self.logger.info("Tomando snapshot de todas las bases de datos fuente...")
        self.load_failed_inserts()
        if self.failed_inserts_log:
            self.logger.info(f"Procesando {len(self.failed_inserts_log)} inserts fallidos antes del snapshot...")
            try:
                target_conn = self.get_db_connection(self.target_config)
                # Crear/actualizar tablas de destino si es necesario
                self.create_target_tables(target_conn)
                # Deshabilitar restricciones
                target_conn.cmd_query("SET foreign_key_checks = 0")
                target_conn.cmd_query("SET sql_mode = ''")
                
                self.process_failed_inserts(target_conn)
                self.save_failed_inserts()  # Guardar log actualizado
                target_conn.close()
            except Exception as e:
                self.logger.error(f"Error procesando inserts fallidos en apertura: {e}")
        
        
        try:
            snapshot = {
                'timestamp': datetime.now().isoformat(),
                'sources': {}
            }
            
            for source_config in self.source_databases:
                self.logger.info(f"Procesando fuente: {source_config['alias']}")
                
                conn = self.get_db_connection(source_config)
                table_info = self.get_table_info(conn, source_config['database'])
                
                source_snapshot = {
                    'database': source_config['database'],
                    'alias': source_config['alias'],
                    'tables': {}
                }
                
                for table_name, info in table_info.items():
                    cursor = conn.cursor(dictionary=True)
                    cursor.execute(f"SELECT * FROM `{table_name}`")
                    rows = cursor.fetchall()
                    
                    # Convertir tipos no serializables
                    table_data = []
                    for row in rows:
                        serializable_row = {}
                        for key, value in row.items():
                            if isinstance(value, datetime):
                                serializable_row[key] = value.isoformat()
                            elif value is None:
                                serializable_row[key] = None
                            else:
                                serializable_row[key] = str(value) if not isinstance(value, (int, float, str, bool)) else value
                        
                        # Agregar hash del registro
                        serializable_row['_record_hash'] = self.generate_record_hash(serializable_row, source_config['alias'])
                        table_data.append(serializable_row)
                    
                    source_snapshot['tables'][table_name] = {
                        'data': table_data,
                        'all_columns': info['all_columns']
                    }
                    
                    cursor.close()
                
                snapshot['sources'][source_config['alias']] = source_snapshot
                conn.close()
            
            # Guardar snapshot
            with open(self.snapshot_file, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, indent=2, default=str)
            
            total_sources = len(self.source_databases)
            self.logger.info(f"Snapshot guardado con {total_sources} fuentes de datos")
            
        except Exception as e:
            self.logger.error(f"Error tomando snapshot: {e}")
            raise
    
    def consolidate_changes(self):
        """Consolida cambios de todas las fuentes en la base de datos de destino"""
        self.logger.info("Iniciando consolidación de cambios...")
        
        # Cargar snapshot
        if not Path(self.snapshot_file).exists():
            self.logger.warning("No existe snapshot previo")
            return
        
        with open(self.snapshot_file, 'r', encoding='utf-8') as f:
            snapshot = json.load(f)
        
        # Cargar inserts fallidos previos
        self.load_failed_inserts()
        
        try:
            target_conn = self.get_db_connection(self.target_config)
            
            # Crear/actualizar tablas de destino
            self.create_target_tables(target_conn)
            
            # Deshabilitar restricciones para inserción libre
            target_conn.cmd_query("SET foreign_key_checks = 0")
            target_conn.cmd_query("SET sql_mode = ''")  # Modo permisivo
            
            # Procesar inserts fallidos primero
            if self.failed_inserts_log:
                self.logger.info(f"Procesando {len(self.failed_inserts_log)} inserts fallidos previos...")
                self.process_failed_inserts(target_conn)
            
            # Procesar cada fuente
            for source_config in self.source_databases:
                self.logger.info(f"Consolidando cambios de: {source_config['alias']}")
                
                source_conn = self.get_db_connection(source_config)
                source_alias = source_config['alias']
                
                if source_alias not in snapshot['sources']:
                    self.logger.warning(f"Fuente {source_alias} no existe en snapshot")
                    continue
                
                table_info = self.get_table_info(source_conn, source_config['database'])
                
                for table_name, info in table_info.items():
                    if table_name not in snapshot['sources'][source_alias]['tables']:
                        self.logger.warning(f"Tabla {table_name} no existe en snapshot de {source_alias}")
                        continue
                    
                    new_records = self.find_new_records(
                        source_conn, table_name,
                        snapshot['sources'][source_alias]['tables'][table_name],
                        info, source_alias
                    )
                    
                    if new_records:
                        self.insert_consolidated_records(target_conn, table_name, new_records, source_config)
                        self.logger.info(f"Consolidados {len(new_records)} nuevos registros de {source_alias}.{table_name}")
                
                source_conn.close()
            
            # Guardar inserts fallidos
            self.save_failed_inserts()
            
            target_conn.close()
            
            self.logger.info("Consolidación completada")
            
        except Exception as e:
            self.logger.error(f"Error en consolidación: {e}")
            raise
    
    def find_new_records(self, conn: mysql.connector.MySQLConnection, table_name: str,
                        snapshot_table: Dict, table_info: Dict, source_alias: str) -> List[Dict]:
        """Encuentra registros nuevos comparando con snapshot"""
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT * FROM `{table_name}`")
        current_rows = cursor.fetchall()
        
        # Crear set de hashes del snapshot
        snapshot_hashes = set()
        for row in snapshot_table['data']:
            if '_record_hash' in row:
                snapshot_hashes.add(row['_record_hash'])
        
        # Encontrar registros nuevos
        new_records = []
        for row in current_rows:
            # Convertir tipos para comparación y hash
            processed_row = {}
            for column in table_info['all_columns']:
                value = row.get(column)
                if isinstance(value, datetime):
                    processed_row[column] = value.isoformat()
                elif value is None:
                    processed_row[column] = None
                else:
                    processed_row[column] = str(value) if not isinstance(value, (int, float, str, bool)) else value
            
            record_hash = self.generate_record_hash(processed_row, source_alias)
            
            if record_hash not in snapshot_hashes:
                # Mantener valores originales para inserción
                original_row = {}
                for column in table_info['all_columns']:
                    original_row[column] = row.get(column)
                
                original_row['_record_hash'] = record_hash
                new_records.append(original_row)
        
        cursor.close()
        return new_records
    
    def insert_consolidated_records(self, conn: mysql.connector.MySQLConnection, 
                                   table_name: str, records: List[Dict], source_config: Dict):
        """Inserta registros en la tabla consolidada"""
        if not records:
            return
        
        successful_inserts = 0
        
        for record in records:
            try:
                # Preparar datos de inserción
                insert_data = record.copy()
                insert_data['_source_database'] = source_config['database']
                insert_data['_source_alias'] = source_config['alias']
                insert_data['_sync_timestamp'] = datetime.now()
                
                # Crear query de inserción
                columns = list(insert_data.keys())
                placeholders = ','.join(['%s' for _ in columns])
                values = [insert_data[col] for col in columns]
                
                query = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES ({placeholders})"
                
                cursor = conn.cursor()
                cursor.execute(query, values)
                conn.commit()
                cursor.close()
                
                successful_inserts += 1
                
            except Exception as e:
                self.logger.warning(f"Error insertando registro en {table_name}: {e}")
                conn.rollback()
                
                # Agregar a log de fallos
                failed_insert = {
                    'table': table_name,
                    'record': record,
                    'source_config': source_config,
                    'timestamp': datetime.now().isoformat(),
                    'error': str(e)
                }
                self.failed_inserts_log.append(failed_insert)
        
        if successful_inserts > 0:
            self.logger.info(f"Insertados {successful_inserts} registros consolidados en {table_name}")
    
    def process_failed_inserts(self, conn: mysql.connector.MySQLConnection):
        """Procesa inserts fallidos del log"""
        if not self.failed_inserts_log:
            return
        
        successful_retries = []
        
        for failed_insert in self.failed_inserts_log:
            table_name = failed_insert['table']
            record = failed_insert['record']
            source_config = failed_insert['source_config']
            
            try:
                insert_data = record.copy()
                insert_data['_source_database'] = source_config['database']
                insert_data['_source_alias'] = source_config['alias']
                insert_data['_sync_timestamp'] = datetime.now()
                
                columns = list(insert_data.keys())
                placeholders = ','.join(['%s' for _ in columns])
                values = [insert_data[col] for col in columns]
                
                query = f"INSERT INTO `{table_name}` (`{'`, `'.join(columns)}`) VALUES ({placeholders})"
                
                cursor = conn.cursor()
                cursor.execute(query, values)
                conn.commit()
                cursor.close()
                
                successful_retries.append(failed_insert)
                self.logger.info(f"Retry exitoso para {table_name} desde {source_config['alias']}")
                
            except Exception as e:
                self.logger.warning(f"Retry fallido para {table_name}: {e}")
                conn.rollback()
        
        # Remover retries exitosos del log
        for retry in successful_retries:
            self.failed_inserts_log.remove(retry)
    
    def load_failed_inserts(self):
        """Carga inserts fallidos del archivo de log"""
        if Path(self.log_file).exists():
            try:
                with open(self.log_file, 'r', encoding='utf-8') as f:
                    self.failed_inserts_log = json.load(f)
                self.logger.info(f"Cargados {len(self.failed_inserts_log)} inserts fallidos")
            except Exception as e:
                self.logger.error(f"Error cargando log de fallos: {e}")
                self.failed_inserts_log = []
        else:
            self.failed_inserts_log = []
    
    def save_failed_inserts(self):
        """Guarda inserts fallidos al archivo de log"""
        try:
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(self.failed_inserts_log, f, indent=2, default=str)
            self.logger.info(f"Guardados {len(self.failed_inserts_log)} inserts fallidos")
        except Exception as e:
            self.logger.error(f"Error guardando log de fallos: {e}")

def parse_mysql_config(config_string: str, alias: str = None) -> Dict:
    """Parsea string de configuración MySQL formato: host:user:password:database[:port]"""
    parts = config_string.split(':')
    if len(parts) < 4:
        raise ValueError("Formato de configuración MySQL debe ser: host:user:password:database[:port]")
    
    config = {
        'host': parts[0],
        'user': parts[1],
        'password': parts[2],
        'database': parts[3],
        'alias': alias or f"{parts[0]}_{parts[3]}"
    }
    
    if len(parts) >= 5:
        config['port'] = int(parts[4])
    
    return config

def main():
    parser = argparse.ArgumentParser(description='Consolidador de múltiples bases de datos MySQL')
    parser.add_argument('target', help='Base de datos de destino: host:user:password:database[:port]')
    parser.add_argument('--sources', nargs='+', required=True,
                       help='Bases de datos fuente: alias1=host:user:password:database[:port]')
    parser.add_argument('--modo', choices=['apertura', 'cierre'], required=True,
                       help='Modo de operación: apertura (snapshot) o cierre (consolidation)')
    parser.add_argument('--log-file', default='consolidation_failures.json',
                       help='Archivo de log para inserts fallidos')
    
    args = parser.parse_args()

    try:
        # Parsear configuración de destino
        target_config = parse_mysql_config(args.target, 'target')
        
        # Parsear configuraciones de fuentes
        source_databases = []
        for source_spec in args.sources:
            if '=' in source_spec:
                alias, config_string = source_spec.split('=', 1)
                source_config = parse_mysql_config(config_string, alias)
            else:
                source_config = parse_mysql_config(source_spec)
            
            source_databases.append(source_config)
        
        consolidator = MySQLDBConsolidator(source_databases, target_config, args.log_file)
        
        if args.modo == 'apertura':
            print("Ejecutando modo APERTURA - Tomando snapshot de todas las fuentes...")
            
            consolidator.take_snapshot()
            print("Snapshot completado exitosamente")
            
        elif args.modo == 'cierre':
            print("Ejecutando modo CIERRE - Consolidando cambios...")
            consolidator.consolidate_changes()
            print("Consolidación completada exitosamente")
            
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()