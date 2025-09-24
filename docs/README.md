# 🗃️ Volcado Web - MySQL Database Consolidation System

Sistema web para consolidación de múltiples bases de datos MySQL con interfaz gráfica y API REST.

## 📋 Funcionalidad

### Características Principales
- **Consolidación de Datos**: Unifica múltiples bases de datos MySQL en una central
- **Dos Modos de Operación**:
  - **Apertura**: Crea snapshot de datos actuales
  - **Cierre**: Consolida cambios desde el último snapshot
- **Interfaz Web**: Panel de control intuitivo para gestionar consolidaciones
- **API REST**: Endpoint para automatización e integración
- **Logging Completo**: Registro detallado de operaciones y errores

## 🛠️ Setup

### Dependencias del Sistema
- **XAMPP** (Apache + MySQL + PHP)
- **Python 3.x**
- **Navegador web**

### Dependencias Python
```bash
pip install mysql-connector-python
```

### Instalación
1. **Clonar/copiar** el proyecto en `/Applications/XAMPP/xamppfiles/htdocs/volcado_web/`
2. **Iniciar XAMPP** (Apache + MySQL)
3. **Configurar bases de datos** en `assets/alias.json`
4. **Acceder** a `http://localhost/volcado_web/`

## 📁 Estructura del Proyecto

```
volcado_web/
├── index.html              # Interfaz web principal
├── api.php                 # API REST endpoint
├── sync.py                 # Motor de consolidación Python
├── assets/
│   ├── alias.json          # Configuración de bases de datos
│   ├── scripts.js          # Lógica frontend
│   └── styles.css          # Estilos CSS
├── consolidation_snapshot.json    # Snapshot de datos
├── consolidation_failures.json   # Log de errores
└── db_consolidation.log          # Log detallado
```

## ⚙️ Configuración

### Base de Datos (`assets/alias.json`)
```json
{
  "db_destino": {
    "alias": "",
    "host": "",
    "user": "",
    "password": "",
    "database": ""
  },
  "db_origenes": [
    {
      "alias": "",
      "host": "",
      "user": "", 
      "password": "",
      "database": ""
    }
  ]
}
```

## 🚀 Uso

### Interfaz Web
1. Abrir `http://localhost/volcado_web/`
2. Seleccionar base de datos origen
3. Ingresar contraseñas (si aplica)
4. Hacer clic en **Apertura** o **Cierre**

### API REST
**Endpoint**: `POST /volcado_web/api.php`

**Payload**:
```json
{
  "modo": "apertura|cierre",
  "db_password_origen": "",
  "db_password_destino": "",
  "db_origen": {
    "alias": "",
    "host": "",
    "user": "",
    "password": "",
    "database": ""
  },
  "db_destino": {
    "host": "", 
    "user": "",
    "password": "",
    "database": ""
  }
}
```

### Línea de Comandos
```bash
python3 sync.py host:user:password:target_db \
  --sources "alias=host:user:password:source_db" \
  --modo apertura
```

## 📊 Archivos Generados

- **`consolidation_snapshot.json`**: Snapshot de datos para comparación
- **`consolidation_failures.json`**: Log de inserts fallidos
- **`db_consolidation.log`**: Log completo de operaciones

## 🔧 Dependencias Técnicas

### Backend
- **PHP 7.4+**: Procesamiento de API
- **Python 3.x**: Motor de consolidación
- **MySQL 5.7+**: Base de datos

### Frontend  
- **HTML5/CSS3**: Interfaz de usuario
- **JavaScript ES6+**: Lógica cliente
- **Fetch API**: Comunicación con backend

### Python Packages
```txt
mysql-connector-python>=8.0.0
```

## 🐛 Troubleshooting

**Error 404**: Verificar que Apache esté corriendo y la ruta sea correcta
**Error de permisos**: Asegurar que Python tenga permisos de escritura en el directorio
**Error de conexión MySQL**: Verificar credenciales en `alias.json`

### estructura
```bash
python3 estructura.py host:user:password:source_db \
  host:user:password:target_db
```