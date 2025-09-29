// Flask-based Database Consolidation System Frontend
// Global variables
const dropdown = document.getElementById("miDropdown");
const statusContainer = document.getElementById("status_info");
const logsContainer = document.getElementById("logs_content");

// Load initial data and setup
document.addEventListener('DOMContentLoaded', function() {
    cargarOpciones();
    loadSystemStatus();
});

async function cargarOpciones() {
    try {
        const res = await fetch("/api/databases");
        const data = await res.json();

        if (data.status === 'error') {
            console.error('Error loading databases:', data.message);
            showNotification('Error cargando configuración de bases de datos', 'error');
            return;
        }

        // Clear existing options except the first one
        dropdown.innerHTML = '<option value="">-- Seleccionar sucursal --</option>';

        data.db_origenes.forEach(origen => {
            const option = document.createElement("option");
            option.value = origen.alias;
            option.textContent = `${origen.alias} (${origen.database})`;
            dropdown.appendChild(option);
        });

        showNotification('Configuración cargada exitosamente', 'success');
    } catch (error) {
        console.error('Error loading options:', error);
        showNotification('Error de conexión al cargar configuración', 'error');
    }
}

async function loadSystemStatus() {
    try {
        const res = await fetch("/api/status");
        const data = await res.json();

        if (data.status === 'error') {
            statusContainer.innerHTML = '<p>🔴 Error obteniendo estado del sistema</p>';
            return;
        }

        statusContainer.innerHTML = `
            <p>🟢 Sistema en línea - v${data.version || '2.0.0'}</p>
            ${data.last_snapshot ? `<p>📸 Último snapshot: ${new Date(data.last_snapshot * 1000).toLocaleString()}</p>` : ''}
        `;

        if (data.recent_logs && data.recent_logs.length > 0) {
            logsContainer.innerHTML = '<pre>' + data.recent_logs.slice(-5).join('') + '</pre>';
        }
    } catch (error) {
        console.error('Error loading status:', error);
        statusContainer.innerHTML = '<p>🔴 Error de conexión</p>';
    }
}

const input_origen = document.getElementById("db_password_origen");
input_origen.addEventListener("input", (e) => {
    console.log(e.target.value);
});

const input_destino = document.getElementById("db_password_destino");
input_destino.addEventListener("input", (e) => {
    console.log(e.target.value);
});

// Utility functions
function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    
    // Add to page
    document.body.appendChild(notification);
    
    // Auto remove after 5 seconds
    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
}

function setButtonState(button, loading = false) {
    if (loading) {
        button.disabled = true;
        button.dataset.originalText = button.textContent;
        button.textContent = '⏳ Procesando...';
    } else {
        button.disabled = false;
        button.textContent = button.dataset.originalText || button.textContent;
    }
}

const get_origen_by_alias = async (alias) => {
    try {
        const res = await fetch("/api/databases");
        const data = await res.json();
        return data.db_origenes.find(origen => origen.alias === alias);
    } catch (error) {
        console.error('Error getting origen by alias:', error);
        throw error;
    }
}

const get_destino = async () => {
    try {
        const res = await fetch("/api/databases");
        const data = await res.json();
        return data.db_destino;
    } catch (error) {
        console.error('Error getting destino:', error);
        throw error;
    }
}

const apertura = async () => {
    const button = document.getElementById('btn_apertura');
    
    try {
        console.log("Starting apertura...");
        setButtonState(button, true);
        
        if (!dropdown.value) {
            showNotification("Por favor selecciona una base de datos de origen", "error");
            return;
        }
        
        const origen = await get_origen_by_alias(dropdown.value);
        const destino = await get_destino();
        
        if (!origen) {
            showNotification("Error: No se pudo encontrar la base de datos de origen seleccionada", "error");
            return;
        }
        
        const post_json = {
            "modo": "apertura",
            "db_password_origen": input_origen.value,
            "db_password_destino": input_destino.value,
            "db_origen": origen,
            "db_destino": destino
        };
        
        console.log("Sending apertura request:", post_json);
        showNotification("Iniciando proceso de apertura...", "info");
        
        const response = await fetch("/api/sync_db", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(post_json)
        });
        
        const result = await response.json();
        console.log("Apertura response:", result);
        
        if (response.ok && result.status !== 'error') {
            showNotification("✅ Apertura completada exitosamente", "success");
            loadSystemStatus(); // Refresh status
        } else {
            const errorMsg = result.message || "Error desconocido";
            showNotification(`❌ Error en apertura: ${errorMsg}`, "error");
        }
    } catch (error) {
        console.error("Error en apertura:", error);
        showNotification(`❌ Error de conexión: ${error.message}`, "error");
    } finally {
        setButtonState(button, false);
    }
}

const cierre = async () => {
    const button = document.getElementById('btn_cierre');
    
    try {
        console.log("Starting cierre...");
        setButtonState(button, true);
        
        if (!dropdown.value) {
            showNotification("Por favor selecciona una base de datos de origen", "error");
            return;
        }
        
        const origen = await get_origen_by_alias(dropdown.value);
        const destino = await get_destino();
        
        if (!origen) {
            showNotification("Error: No se pudo encontrar la base de datos de origen seleccionada", "error");
            return;
        }
        
        const post_json = {
            "modo": "cierre",
            "db_password_origen": input_origen.value,
            "db_password_destino": input_destino.value,
            "db_origen": origen,
            "db_destino": destino
        };
        
        console.log("Sending cierre request:", post_json);
        showNotification("Iniciando proceso de cierre...", "info");
        
        const response = await fetch("/api/sync_db", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(post_json)
        });
        
        const result = await response.json();
        console.log("Cierre response:", result);
        
        if (response.ok && result.status !== 'error') {
            showNotification("✅ Cierre completado exitosamente", "success");
            loadSystemStatus(); // Refresh status
        } else {
            const errorMsg = result.message || "Error desconocido";
            showNotification(`❌ Error en cierre: ${errorMsg}`, "error");
        }
    } catch (error) {
        console.error("Error en cierre:", error);
        showNotification(`❌ Error de conexión: ${error.message}`, "error");
    } finally {
        setButtonState(button, false);
    }
}