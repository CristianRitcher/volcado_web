#!/usr/bin/env python3
"""
Script simple de sincronización de estructura de bases de datos MySQL.
Toma una DB origen y sincroniza estructura con una DB destino.
"""

import mysql.connector
from mysql.connector import Error
import argparse
import sys
from typing import Dict

class SimpleStructureSync:
    def __init__(self, source_config: Dict, target_config: Dict):
        self.source_config = source_config
        self.target_config = target_config
        
        # Columnas de metadatos estándar
        self.metadata_columns = {
            '_consolidation_id': 'BIGINT AUTO_INCREMENT PRIMARY KEY',
            '_source_database': 'VARCHAR(255) NOT NULL',
            '_source_alias': 'VARCHAR(100) NOT NULL',
            '_sync_timestamp': 'DATETIME DEFAULT CURRENT_TIMESTAMP',
            '_record_hash': 'VARCHAR(64) NOT NULL'
        }
    
    def get_db_connection(self, db_config: Dict) -> mysql.connector.MySQLConnection:
        """Obtiene conexión a la base de datos MySQL"""
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
    
    def get_table_structure(self, conn: mysql.connector.MySQLConnection, database: str) -> Dict[str, Dict]:
        """Obtiene estructura completa de todas las tablas"""
        cursor = conn.cursor(dictionary=True)
        
        # Obtener todas las tablas
        cursor.execute("""
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = %s 
            AND TABLE_TYPE = 'BASE TABLE'
        """, (database,))
        tables = [row['TABLE_NAME'] for row in cursor.fetchall()]
        
        table_structure = {}
        for table in tables:
            # Obtener información de columnas
            cursor.execute("""
                SELECT 
                    COLUMN_NAME,
                    COLUMN_TYPE,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (database, table))
            columns_info = cursor.fetchall()
            
            table_structure[table] = {
                'columns': {col['COLUMN_NAME']: col for col in columns_info},
                'column_order': [col['COLUMN_NAME'] for col in columns_info]
            }
        
        cursor.close()
        return table_structure
    
    def convert_to_flexible_mysql_type(self, column_info: Dict) -> str:
        """Convierte definición de columna a tipo MySQL flexible"""
        data_type = column_info['DATA_TYPE'].lower()
        
        type_mapping = {
            'tinyint': 'TINYINT',
            'smallint': 'SMALLINT', 
            'mediumint': 'MEDIUMINT',
            'int': 'INT',
            'integer': 'INT',
            'bigint': 'BIGINT',
            'float': 'FLOAT',
            'double': 'DOUBLE',
            'decimal': 'DECIMAL(15,4)',
            'date': 'DATE',
            'time': 'TIME',
            'datetime': 'DATETIME',
            'timestamp': 'TIMESTAMP',
            'year': 'YEAR',
            'char': 'TEXT',
            'varchar': 'TEXT',
            'text': 'TEXT',
            'tinytext': 'TEXT',
            'mediumtext': 'MEDIUMTEXT',
            'longtext': 'LONGTEXT',
            'binary': 'VARBINARY(255)',
            'varbinary': 'VARBINARY(255)', 
            'blob': 'BLOB',
            'tinyblob': 'BLOB',
            'mediumblob': 'MEDIUMBLOB',
            'longblob': 'LONGBLOB',
            'json': 'JSON',
            'boolean': 'BOOLEAN',
            'bool': 'BOOLEAN'
        }
        
        mysql_type = 'TEXT'  # Fallback
        for key, value in type_mapping.items():
            if key in data_type:
                mysql_type = value
                break
        
        return f"{mysql_type} NULL"
    
    def sync(self):
        """Sincroniza estructura de base de datos"""
        # Conectar a ambas bases
        source_conn = self.get_db_connection(self.source_config)
        target_conn = self.get_db_connection(self.target_config)
        
        # Obtener estructuras
        source_structure = self.get_table_structure(source_conn, self.source_config['database'])
        target_structure = self.get_table_structure(target_conn, self.target_config['database'])
        
        cursor = target_conn.cursor()
        
        # Procesar cada tabla de origen
        for table_name, table_schema in source_structure.items():
            
            if table_name not in target_structure:
                # Crear tabla completa
                self.create_table_with_metadata(cursor, table_name, table_schema, target_conn)
            else:
                # Actualizar tabla existente
                self.update_table_structure(cursor, table_name, table_schema, 
                                          target_structure[table_name], target_conn)
        
        cursor.close()
        source_conn.close()
        target_conn.close()
    
    def create_table_with_metadata(self, cursor, table_name: str, table_schema: Dict, conn):
        """Crea una nueva tabla con metadatos"""
        columns_sql = []
        
        # Columnas de metadatos primero
        for meta_col, meta_type in self.metadata_columns.items():
            columns_sql.append(f"`{meta_col}` {meta_type}")
        
        # Columnas originales
        for col_name in table_schema['column_order']:
            if col_name in table_schema['columns']:
                col_info = table_schema['columns'][col_name]
                mysql_type = self.convert_to_flexible_mysql_type(col_info)
                columns_sql.append(f"`{col_name}` {mysql_type}")
        
        # Índices
        indexes_sql = [
            "INDEX `idx_source_alias` (`_source_alias`)",
            "INDEX `idx_sync_timestamp` (`_sync_timestamp`)", 
            "INDEX `idx_record_hash` (`_record_hash`)"
        ]
        
        create_sql = f"""
            CREATE TABLE `{table_name}` (
                {', '.join(columns_sql + indexes_sql)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        cursor.execute(create_sql)
        conn.commit()
    
    def update_table_structure(self, cursor, table_name: str, source_schema: Dict, 
                             target_structure: Dict, conn):
        """Actualiza estructura de tabla existente"""
        current_columns = set(target_structure['columns'].keys())
        source_columns = set(source_schema['columns'].keys())
        metadata_columns = set(self.metadata_columns.keys())
        
        # Agregar columnas de metadatos faltantes
        missing_metadata = metadata_columns - current_columns
        for meta_col in missing_metadata:
            meta_type = self.metadata_columns[meta_col]
            alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{meta_col}` {meta_type}"
            cursor.execute(alter_sql)
            conn.commit()
        
        # Agregar columnas originales faltantes
        missing_source = source_columns - current_columns
        for col_name in missing_source:
            col_info = source_schema['columns'][col_name]
            mysql_type = self.convert_to_flexible_mysql_type(col_info)
            alter_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {mysql_type}"
            cursor.execute(alter_sql)
            conn.commit()
        
        # Asegurar índices de metadatos
        self.ensure_metadata_indexes(cursor, table_name, conn)
    
    def ensure_metadata_indexes(self, cursor, table_name: str, conn):
        """Crea índices de metadatos si no existen"""
        cursor.execute(f"SHOW INDEX FROM `{table_name}`")
        existing_indexes = set(row['Key_name'] for row in cursor.fetchall())
        
        required_indexes = {
            'idx_source_alias': f'CREATE INDEX `idx_source_alias` ON `{table_name}` (`_source_alias`)',
            'idx_sync_timestamp': f'CREATE INDEX `idx_sync_timestamp` ON `{table_name}` (`_sync_timestamp`)',
            'idx_record_hash': f'CREATE INDEX `idx_record_hash` ON `{table_name}` (`_record_hash`)'
        }
        
        for index_name, index_sql in required_indexes.items():
            if index_name not in existing_indexes:
                try:
                    cursor.execute(index_sql)
                    conn.commit()
                except mysql.connector.Error:
                    pass  # Ignorar errores de índices duplicados

def parse_mysql_config(config_string: str) -> Dict:
    """Parsea configuración MySQL: host:user:password:database[:port]"""
    parts = config_string.split(':')
    if len(parts) < 4:
        raise ValueError("Formato debe ser: host:user:password:database[:port]")
    
    config = {
        'host': parts[0],
        'user': parts[1],
        'password': parts[2],
        'database': parts[3]
    }
    
    if len(parts) >= 5:
        config['port'] = int(parts[4])
    
    return config

def main():
    parser = argparse.ArgumentParser(description='Sincronización simple de estructura MySQL')
    parser.add_argument('source', help='DB origen: host:user:password:database[:port]')
    parser.add_argument('target', help='DB destino: host:user:password:database[:port]')
    
    args = parser.parse_args()
    
    try:
        source_config = parse_mysql_config(args.source)
        target_config = parse_mysql_config(args.target)
        
        syncer = SimpleStructureSync(source_config, target_config)
        syncer.sync()
        
        print("Estructura sincronizada exitosamente")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()