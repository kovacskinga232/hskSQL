const API_URL = "http://127.0.0.1:8000";
const dbName = localStorage.getItem("dbName");

document.addEventListener("DOMContentLoaded", () => {
    if (dbName) {
        document.getElementById("dbName").textContent = `${dbName} database`;
        loadTables(dbName);
    }
    setupEventHandlers();
});

/* EVENT HANDLERS */
function setupEventHandlers() {
    const $ = selector => document.querySelector(selector);

    $("#goBack").onclick = () => location.href = "index.html";
    $("#createTable").onclick = () => $(".createPopup").classList.add("active");
    $(".createPopup .closeButton").onclick = () => {
        clearCreateForm();
        $(".createPopup").classList.remove("active");
    };

    $("#addButton").onclick = addAttributeCard;

    $("#dropTable").onclick = () => $(".dropPopup").classList.add("active");
    $(".dropPopup .closeButton").onclick = () => {
        $("#dropTableName").value = "";
        $(".dropPopup").classList.remove("active");
    };

    $("#drop").onclick = dropTable;
    $("#create").onclick = createTable;
}

/* FORM UTILITIES */
function clearCreateForm() {
    document.getElementById("tableName").value = "";
    document.querySelector(".cardContainer").innerHTML = "";
}

function addAttributeCard() {
    const container = document.querySelector(".cardContainer");
    const card = document.createElement("div");
    card.className = "createCard";

    const nameInput = document.createElement("input");
    nameInput.type = "text";
    nameInput.placeholder = "Attribute name";
    nameInput.className = "attributeName";

    const typeSelect = document.createElement("select");
    typeSelect.className = "attributeType";
    ["int", "float", "bit", "date", "datetime", "varchar"].forEach(type => {
        const option = document.createElement("option");
        option.value = type;
        option.textContent = type.charAt(0).toUpperCase() + type.slice(1);
        typeSelect.appendChild(option);
    });

    /* primary key section */
    const pkSection = document.createElement("div");
    pkSection.className = "primaryKeySection";

    const pkP = document.createElement("p");
    pkP.textContent = "Primary key";

    const pkRadio = document.createElement("input");
    pkRadio.type = "radio";
    pkRadio.name = "primaryKey";
    pkRadio.className = "primaryKeyRadio";

    pkSection.append(pkP, pkRadio);

    // unique constraint
    const uSection = document.createElement("div");
    uSection.className = "uniqueSection";

    const uP = document.createElement("p");
    uP.textContent = "Uniqueness";

    const ukCheckbox = document.createElement("input");
    ukCheckbox.type = "checkbox";
    ukCheckbox.name = "unique";
    ukCheckbox.className = "uniqueCheckbox";

    uSection.append(uP, ukCheckbox);

    /* foreign key section*/
    const fkSection = document.createElement("div");
    fkSection.className = "foreignKeySection";

    const fkP = document.createElement("p");
    fkP.textContent = "Foreign key";

    const fkTable = document.createElement("input");
    fkTable.type = "text";
    fkTable.placeholder = "Reference Table";
    fkTable.className = "fkTable";

    const fkColumn = document.createElement("input");
    fkColumn.type = "text";
    fkColumn.placeholder = "Reference Column";
    fkColumn.className = "fkColumn";

    fkSection.append(fkP, fkTable, fkColumn);

    /* remove button */
    const removeBtn = document.createElement("button");
    removeBtn.textContent = "x";
    removeBtn.className = "removeButton";

    // add event listeners to the elements
    removeBtn.onclick = () => container.removeChild(card);

    pkRadio.onchange = () => {
        document.querySelectorAll(".primaryKeyRadio").forEach(r => r.checked = false);
        pkRadio.checked = true;
    };

    ukCheckbox.addEventListener("change", () => {
        if (ukCheckbox.checked) {
            uSection.classList.add("checked");
        } else {
            uSection.classList.remove("checked");
        }
    });

    card.append(nameInput, typeSelect, pkSection, uSection, fkSection, removeBtn);
    container.appendChild(card);
}

/* DATABASE OPERATIONS */
async function loadTables(databaseName) {
    try {
        const res = await fetch(`${API_URL}/get_database?database_name=${encodeURIComponent(databaseName)}`);
        const { tables } = await res.json();
        const container = document.getElementById("tablesContainer");
        container.innerHTML = "";

        Object.keys(tables).forEach(tableName => {
            const btn = document.createElement("button");
            btn.className = "table-button";
            btn.textContent = tableName;
            btn.onclick = () => {
                localStorage.setItem("selectedTable", tableName);
                location.href = "table_details.html";
            };
            container.appendChild(btn);
        });
    } catch (err) {
        console.error("Failed to load tables:", err);
    }
}

// create table with the given attributes and constraints
async function createTable() {
    const tableName = document.getElementById("tableName").value.trim();
    const cards = document.querySelectorAll(".createCard");
    const unique_constraints = [];
    const attributes = [];
    const foreign_keys = {};
    let primaryKey = null;

    cards.forEach(card => {
        const name = card.querySelector(".attributeName").value.trim();
        const type = card.querySelector(".attributeType").value;
        const isPK = card.querySelector(".primaryKeyRadio").checked;
        const hasU = card.querySelector(".uniqueCheckbox")?.checked;
        const fkTable = card.querySelector(".fkTable").value.trim();
        const fkColumn = card.querySelector(".fkColumn").value.trim();

        if (name && type) {
            attributes.push({ name, type });
            if (isPK) primaryKey = name;
            if (hasU) unique_constraints.push(name);
            if (fkTable && fkColumn) {
                foreign_keys[`fk_${tableName}_${name}`] = {
                    column_name: name,
                    reference_table: fkTable,
                    reference_column: fkColumn
                };
            }
        }
    });

    if (!tableName || attributes.length === 0 || !primaryKey) {
        alert("Please fill in all fields and select a primary key.");
        return;
    }

    try {
        const res = await fetch(`${API_URL}/create_table`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                database_name: dbName,
                table_name: tableName,
                columns: Object.fromEntries(attributes.map(attr => [attr.name, attr.type])),
                primary_key: primaryKey,
                unique_constraints,
                foreign_keys
            })
        });

        const result = await res.json();
        alert(result.message || result.detail);
        clearCreateForm();
        document.querySelector(".createPopup").classList.remove("active");
        loadTables(dbName);
    } catch (err) {
        console.error("Failed to create table:", err);
        alert("Error occurred while creating the table.");
    }
}

// drop table based on its name
async function dropTable() {
    const tableName = document.getElementById("dropTableName").value;

    try {
            const response = await fetch(`${API_URL}/drop_table`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                database_name: dbName,
                table_name: tableName
            })
        });

        const result = await response.json();

        if (response.ok) {
            alert(result.message); 
            loadTables(dbName);
            document.querySelector(".dropPopup").classList.remove("active");
        } else {
            alert(result.detail || "Failed to drop the table.");
        }
    } catch (error) {
        console.error("Error dropping table:", error);
        alert("Failed to drop the table. Please try again.");
    }
}
