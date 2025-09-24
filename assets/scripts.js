// Declare dropdown globally so it can be accessed by button functions
const dropdown = document.getElementById("miDropdown");

async function cargarOpciones() {
    const res = await fetch("assets/alias.json");
    const data = await res.json();

    data.db_origenes.forEach(origen => {
        const option = document.createElement("option");
        option.value = origen.alias;
        option.textContent = origen.alias;
        dropdown.appendChild(option);
    });
}
cargarOpciones();

const input_origen = document.getElementById("db_password_origen");
input_origen.addEventListener("input", (e) => {
    console.log(e.target.value);
});

const input_destino = document.getElementById("db_password_destino");
input_destino.addEventListener("input", (e) => {
    console.log(e.target.value);
});

const get_origen_by_alias = async (alias) => {
    const res = await fetch("assets/alias.json");
    const data = await res.json();
    return data.db_origenes.find(origen => origen.alias === alias);
}

const get_destino = async () => {
    const res = await fetch("assets/alias.json");
    const data = await res.json();
    return data.db_destino;
}

const apertura = async () => {
    try {
        console.log("Starting apertura...");
        const origen = await get_origen_by_alias(dropdown.value);
        const destino = await get_destino();
        
        if (!origen) {
            alert("Error: No se pudo encontrar la base de datos de origen seleccionada");
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
        
        const response = await fetch("api.php", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(post_json)
        });
        
        const result = await response.text();
        console.log("Apertura response:", result);
        
        if (response.ok) {
            alert("Apertura completada exitosamente");
        } else {
            alert("Error en apertura: " + result);
        }
    } catch (error) {
        console.error("Error en apertura:", error);
        alert("Error en apertura: " + error.message);
    }
}

const cierre = async () => {
    try {
        console.log("Starting cierre...");
        const origen = await get_origen_by_alias(dropdown.value);
        const destino = await get_destino();
        
        if (!origen) {
            alert("Error: No se pudo encontrar la base de datos de origen seleccionada");
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
        
        const response = await fetch("api.php", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(post_json)
        });
        
        const result = await response.text();
        console.log("Cierre response:", result);
        
        if (response.ok) {
            alert("Cierre completado exitosamente");
        } else {
            alert("Error en cierre: " + result);
        }
    } catch (error) {
        console.error("Error en cierre:", error);
        alert("Error en cierre: " + error.message);
    }
}