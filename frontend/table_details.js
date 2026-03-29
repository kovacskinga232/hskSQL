let currentPage = 1;
const pageSize = 10;

const operators = ["=", ">", "<", ">=", "<="];

let databaseName = localStorage.getItem("dbName");
let tableName = localStorage.getItem("selectedTable");

// DOM Utilities
const showPopup = (selector) => document.querySelector(selector).classList.add("active");
const hidePopup = (selector) => document.querySelector(selector).classList.remove("active");

// Core Functions
async function fetchData(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch: ${url}`);
  return await response.json();
}

async function postData(url, data) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data)
  });
  if (!response.ok) throw new Error(await response.text());
  return await response.json();
}

// Table Operations
async function createTableStructure() {
  const tableDiv = document.getElementById("tableStructure");
  tableDiv.innerHTML = "Loading...";

  try {
    const [columns, data] = await Promise.all([
      fetchData(`http://127.0.0.1:8000/get_columns?database_name=${databaseName}&table_name=${tableName}`),
      fetchData(`http://127.0.0.1:8000/list_data?database_name=${databaseName}&table_name=${tableName}`)
    ]);

    const headers = Object.keys(columns.columns);
    const records = data.records ?? [];
    const totalPages = Math.ceil(records.length / pageSize);
    currentPage = Math.max(1, Math.min(currentPage, totalPages));

    const table = document.createElement("table");
    table.className = "data-table";

    // Create headers
    const thead = document.createElement("thead");
    thead.innerHTML = `<tr>${headers.map(h => `<th>${h}</th>`).join("")}</tr>`;
    table.appendChild(thead);

    // Create body with paginated data
    const tbody = document.createElement("tbody");
    const start = (currentPage - 1) * pageSize;
    const end = start + pageSize;
    const pageData = records.slice(start, end);

    if (pageData.length) {
      tbody.innerHTML = pageData.map(r =>
        `<tr>${headers.map(h => `<td>${r[h] ?? ""}</td>`).join("")}</tr>`
      ).join("");
    } else {
      tbody.innerHTML = `<tr><td colspan="${headers.length}">No data</td></tr>`;
    }

    table.appendChild(tbody);

    // Clear and add table
    tableDiv.innerHTML = "";
    tableDiv.appendChild(table);

    // Add pagination controls
    const pagination = document.createElement("div");
    pagination.className = "pagination";
    pagination.innerHTML = `
      <button ${currentPage === 1 ? "disabled" : ""} onclick="prevPage()">«</button>
      <span>Page ${currentPage} of ${totalPages}</span>
      <button ${currentPage === totalPages ? "disabled" : ""} onclick="nextPage()">»</button>
    `;
    tableDiv.appendChild(pagination);

  } catch (error) {
    tableDiv.textContent = `Error loading table data: ${error.message}`;
    console.error(error);
  }
}

function prevPage() {
  if (currentPage > 1) {
    currentPage--;
    createTableStructure();
  }
}

function nextPage() {
  currentPage++;
  createTableStructure();
}

// INSERT FORM
async function renderInsertForm() {
    const container = document.querySelector(".insertPopup .cardContainer");
    container.innerHTML = ""; // clear existing content

    if (!databaseName || !tableName) {
        alert("Missing database or table name.");
        return;
    }

    try {
        const response = await fetch(`http://127.0.0.1:8000/get_columns?database_name=${databaseName}&table_name=${tableName}`);
        const columnData = await response.json();
        const columns = columnData.columns;

        const pkRes = await fetch(`http://127.0.0.1:8000/get_primary_key?database_name=${databaseName}&table_name=${tableName}`);
        const pkData = await pkRes.json();
        const primaryKey = pkData.primary_key;

        // store columns for later use when adding new cards
        window.insertFormColumns = columns;
        window.insertFormPrimaryKey = primaryKey;

        // create initial card
        createInputCard(container, columns, primaryKey);
    } catch (err) {
        console.error("Failed to render insert form:", err);
        alert("Error loading table details for insert.");
    }
}

// insert
async function handleInsertClick() {
    const container = document.querySelector(".insertPopup .cardContainer");
    const allCards = container.querySelectorAll(".input-card");
    const records = [];

    allCards.forEach(card => {
        const inputs = card.querySelectorAll("input");
        const record = {};
        
        inputs.forEach(input => {
            if (input.type === "checkbox") {
                record[input.name] = input.checked ? true : false;
            } else if (input.value.trim() !== "") {
                record[input.name] = input.value;
            }
        });

        if (Object.keys(record).length > 0) {
            records.push(record);
        }
    });

    if (records.length === 0) {
        alert("Please enter at least one value to insert.");
        return;
    }

    try {
        const response = await fetch(`http://127.0.0.1:8000/insert`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                database_name: databaseName,
                table_name: tableName,
                records: records
            })
        });

        const result = await response.json();
        
        if (response.ok) {
            if (result.inserted_count > 0) {
                alert(result.message || "Row inserted successfully.");
                await createTableStructure(databaseName, tableName);
            } else if (result.errors && result.errors.length > 0) {
                alert("Insert failed: " + result.errors.join("; "));
            } else {
                alert("Insert failed for unknown reasons.");
            }
        } else {
            const errorMessage =
                result?.detail ||
                result?.error ||
                result?.message ||
                JSON.stringify(result) ||
                "Failed to insert row.";
            alert("Error: " + errorMessage);
        }

        hidePopup(".insertPopup");
    } catch (err) {
        console.error("Error inserting rows:", err);
        alert("Failed to insert rows. Please check your input and try again.");
    }
}

// helper function to create a single input card
function createInputCard(container, columns, primaryKey) {
    const card = document.createElement("div");
    card.className = "input-card";
    
    // create inputs for each column
    Object.entries(columns).forEach(([colName, colType]) => {
        const wrapper = document.createElement("div");
        wrapper.className = "input-wrapper";
        
        const label = document.createElement("label");
        label.textContent = `${colName} (${colType})${colName === primaryKey ? " (PK)" : ""}`;
        
        const input = document.createElement("input");
        input.name = colName;

        // Choose appropriate input type based on colType
        switch (colType.toLowerCase()) {
            case "int":
            case "float":
                input.type = "number";
                if (colType === "int") input.step = "1";
                else input.step = "any"; // for float
                break;
            case "date":
                input.type = "date";
                break;
            case "datetime":
                input.type = "datetime-local";
                break;
            case "bit":
                input.type = "checkbox";
                break;
            case "varchar":
            default:
                input.type = "text";
                break;
        }

        if (input.type !== "checkbox") {
            input.placeholder = `Enter ${colName}`;
        }

        wrapper.appendChild(label);
        wrapper.appendChild(input);
        card.appendChild(wrapper);
    });

    // add a remove button for this card
    const removeBtn = document.createElement("button");
    removeBtn.textContent = "×";
    removeBtn.className = "remove-card";
    removeBtn.addEventListener("click", () => {
        card.remove();
    });
    card.appendChild(removeBtn);
    
    container.appendChild(card);
}

// insert values
async function insertTableValues(form) {
    const row = {};
    const formData = new FormData(form);
    for (const [key, value] of formData.entries()) {
        row[key] = value;
    }

    try {
        const response = await fetch(`http://127.0.0.1:8000/insert`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                database_name: databaseName,
                table_name: tableName,
                records: [row]
            })
        });

        const result = await response.json();
        if (response.ok) {
            alert(result.message || "Row inserted successfully.");
            await createTableStructure(databaseName, tableName);
        } else {
            alert(result.detail || "Failed to insert row.");
        }
    } catch (err) {
        console.error("Error inserting row:", err);
        alert("Failed to insert row.");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DELETE
async function setupDeletePopup() {
  const columns = await fetchData(`http://127.0.0.1:8000/get_columns?database_name=${databaseName}&table_name=${tableName}`);

  const container = document.getElementById("conditionContainer");
  container.innerHTML = "";

  document.getElementById('addConditionBtn').onclick = () => addCondition(columns.columns);
  addCondition(columns.columns);
}

// add condition
function addCondition(columns) {
  const container = document.getElementById("conditionContainer");

  const row = document.createElement("div");
  row.className = "condition-row";
  row.style.cssText = "margin-bottom:4px;display:flex;align-items:center;gap:8px";

  const useCheckbox = document.createElement("input");
  useCheckbox.type = "checkbox";
  useCheckbox.className = "use-condition";
  useCheckbox.checked = true;

  const columnSelect = document.createElement("select");
  columnSelect.className = "column";
  for (const col of Object.keys(columns)) {
    const option = document.createElement("option");
    option.value = col;
    option.textContent = col;
    columnSelect.appendChild(option);
  }

  const operatorSelect = document.createElement("select");
  operatorSelect.className = "operator";

  const valueInput = document.createElement("input");
  valueInput.type = "text";
  valueInput.className = "value";

  const removeBtn = document.createElement("button");
  removeBtn.type = "button";
  removeBtn.textContent = "Remove";
  removeBtn.onclick = () => row.remove();

  row.appendChild(useCheckbox);
  row.appendChild(columnSelect);
  row.appendChild(operatorSelect);
  row.appendChild(valueInput);
  row.appendChild(removeBtn);
  container.appendChild(row);

  const updatePlaceholderAndOperators = () => {
    const colName = columnSelect.value;
    const type = columns[colName].toLowerCase();

    // Update placeholder
    if (type.includes("char")) {
      valueInput.placeholder = 'e.g. John';
    } else if (type.includes("int")) {
      valueInput.placeholder = 'e.g. 42';
    } else if (type.includes("float")) {
      valueInput.placeholder = 'e.g. 7.3';
    } else if (type.includes("bit")) {
      valueInput.placeholder = 'true / false';
    } else if (type.includes("date") || type.includes("time")) {
      valueInput.placeholder = 'e.g. 2023-01-01';
    } else {
      valueInput.placeholder = 'Enter value';
    }

    // Update operators
    operatorSelect.innerHTML = "";
    const allowedOperators = type.includes("bit") ? ['='] : operators;
    for (const op of allowedOperators) {
      const opOption = document.createElement("option");
      opOption.value = op;
      opOption.textContent = op;
      operatorSelect.appendChild(opOption);
    }
  };

  // Initial setup
  updatePlaceholderAndOperators();
  columnSelect.addEventListener("change", updatePlaceholderAndOperators);
}

async function handleDelete() {
  const deleteButton = document.getElementById('delete');
  try {
    deleteButton.disabled = true;
    deleteButton.textContent = "Deleting...";

    // Get conditions
    const conditions = [...document.querySelectorAll('#conditionContainer .condition-row')]
    .filter(row => row.querySelector('.use-condition').checked)
    .map(row => {
        return {
        column: row.querySelector('.column').value,
        operator: row.querySelector('.operator').value,
        value: row.querySelector('.value').value.trim()
        };
    })
    .filter(cond => cond.value !== '' && cond.value !== null && cond.value !== undefined);

    if (!conditions.length) {
      throw new Error("Please provide conditions");
    }

    const response = await fetch('http://127.0.0.1:8000/delete', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify({
        database_name: databaseName,
        table_name: tableName,
        conditions: conditions.length > 0 ? conditions : null
    })
    });

    console.log(conditions);

    const result = await response.json();

    if (!response.ok) throw new Error(result.detail || result.message || "Delete failed");

    alert(result.message || "Deleted successfully");
    hidePopup(".deletePopup");
    await createTableStructure();
  } catch (error) {
    alert(`Delete failed: ${error.message}`);
  } finally {
    deleteButton.disabled = false;
    deleteButton.textContent = "Delete";
  }
}

function formatCondition(column, operator, value) {
  if (['>', '<', '>=', '<='].includes(operator) && isNaN(value)) {
    alert(`Operator ${operator} requires numeric value`);
    return null;
  }
  return `this.${column} ${operator} ${isNaN(value) ? `"${value}"` : value}`;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FOREIGN KEYS
async function loadForeignKeys(databaseName, tableName) {
  try {
    const response = await fetch(`http://127.0.0.1:8000/list_foreign_keys?database_name=${databaseName}&table_name=${tableName}`);
    const foreignKeysObject = await response.json();
    const foreignKeysRaw = foreignKeysObject.foreign_keys;

    if (typeof foreignKeysRaw !== "object") {
      console.error("Unexpected response for foreign keys:", foreignKeysRaw);
      document.getElementById("foreignKeysContainer").textContent = "No foreign keys or error.";
      return;
    }

    const foreignKeys = Object.entries(foreignKeysRaw).map(([constraintName, info]) => ({
      constraint_name: constraintName,
      column_name: info.column,
      referenced_table: info.reference_table,
      referenced_column: info.reference_column
    }));

    const container = document.getElementById("foreignKeysContainer");
    container.innerHTML = "";

    foreignKeys.forEach(fk => {
      const card = document.createElement("div");
      card.className = "fk-card"; // assign a class for styling
      
      card.innerHTML = `
        <h3 class="fk-title">${fk.constraint_name}</h3>
        <p><strong>Column:</strong> ${fk.column_name}</p>
        <p><strong>References:</strong> ${fk.referenced_table}(${fk.referenced_column})</p>
        <button class="fk-drop-btn">Drop</button>
      `;

      const dropBtn = card.querySelector(".fk-drop-btn");
      dropBtn.onclick = () => dropForeignKey(databaseName, tableName, fk.constraint_name);

      container.appendChild(card);
    });
  } catch (err) {
    console.error("Error loading foreign keys:", err);
    document.getElementById("foreignKeysContainer").textContent = "Failed to load foreign keys.";
  }
}

async function dropForeignKey(database, fkTable, constraintName) {
    try {
        const response = await fetch(`http://127.0.0.1:8000/list_foreign_keys?database_name=${database}&table_name=${fkTable}`);
        const data = await response.json();
        const foreignKeys = data.foreign_keys;

        if (!foreignKeys || !foreignKeys[constraintName]) {
            alert("Foreign key constraint not found.");
            return;
        }

        const fk = foreignKeys[constraintName];
        const confirmDrop = confirm(`Are you sure you want to delete the "${constraintName}" foreign key (${fkTable}.${fk.column_name} → ${fk.reference_table})?`);
        if (!confirmDrop) return;

        const dropResponse = await fetch("http://127.0.0.1:8000/drop_fk_constraint", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                name: constraintName,
                database: database,
                fk_table: fkTable,
                fk_column: fk.column_name,
                reference_table: fk.reference_table
            })
        });

        const result = await dropResponse.json();
        if (!dropResponse.ok) throw new Error(JSON.stringify(result));

        alert(result.message || "Foreign key dropped.");
        await loadForeignKeys(database, fkTable); // refresh the correct table
    } catch (error) {
        console.error("Error while dropping foreign key:", error);
        alert("There was an error while dropping the foreign key.");
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// INDEXES
async function listIndexes() {
    try {
        const response = await fetch(`http://127.0.0.1:8000/list_indexes?database_name=${databaseName}&table_name=${tableName}`);     
        if (!response.ok) {
            throw new Error("Failed to fetch indexes");
        }

        const data = await response.json();
        const indexes = data.indexes;

        const container = document.getElementById("indexStructure");
        container.innerHTML = "";

        const indexEntries = Object.entries(indexes);
        if (indexEntries.length === 0) {
            container.textContent = "No indexes found.";
            return;
        }

        const table = document.createElement("table");
        table.classList.add("structure-table");

        const thead = document.createElement("thead");
        const headerRow = document.createElement("tr");
        ["Name", "Field", "Unique"].forEach(text => {
            const th = document.createElement("th");
            th.textContent = text;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement("tbody");
        for (const [indexName, indexInfo] of indexEntries) {
            const row = document.createElement("tr");

            const nameCell = document.createElement("td");
            nameCell.textContent = indexName;

            const fieldCell = document.createElement("td");
            fieldCell.textContent = indexInfo.field;

            const uniqueCell = document.createElement("td");
            uniqueCell.textContent = indexInfo.unique ? "Yes" : "No";

            row.appendChild(nameCell);
            row.appendChild(fieldCell);
            row.appendChild(uniqueCell);
            tbody.appendChild(row);
        }

        table.appendChild(tbody);
        container.appendChild(table);
    } catch (err) {
        console.error("Failed to fetch indexes:", err);
        document.getElementById("indexStructure").textContent = "Failed to load indexes.";
    }
}

async function dropIndex(indexName) {
    try {
        const response = await fetch("http://127.0.0.1:8000/drop_index", {
            method: "DELETE",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                database_name: databaseName,
                table_name: tableName,
                index_name: indexName
            })
        });

        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || "Failed to drop index");

        alert(result.message);
        await listIndexes();
    } catch (error) {
        console.error("Error dropping index:", error);
        alert("Error dropping index.");
    }
}

async function createIndex(fieldName, isUnique) {
    try {
        const response = await fetch("http://127.0.0.1:8000/create_index", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            database_name: databaseName,
            table_name: tableName,
            field_name: fieldName,
            unique: isUnique
        })
        });

        const result = await response.json();
        if (!response.ok) throw new Error(result.detail || "Failed to create index");

        alert(result.message);
        await listIndexes();
    } catch (error) {
        console.error("Error creating index:", error);
        alert("Error creating index.");
    }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Initialize
document.addEventListener("DOMContentLoaded", async () => {
  document.getElementById("tableTitle").textContent = `${tableName} table`;
  
  // Setup event handlers
  // Insert
  document.getElementById("insertButton").addEventListener("click", () => {
    showPopup(".insertPopup");
    renderInsertForm();
  });

  document.getElementById("insert").addEventListener("click", handleInsertClick);

  const addButton = document.getElementById("addButton");
  if (addButton) {
    addButton.addEventListener("click", () => {
        const container = document.querySelector(".insertPopup .cardContainer");
        const columns = window.insertFormColumns;
        const primaryKey = window.insertFormPrimaryKey;

        if (!columns || !primaryKey) {
            alert("Insert form is not initialized yet.");
            return;
        }

        createInputCard(container, columns, primaryKey);
    });
  }
  
  document.getElementById("deleteButton").addEventListener("click", () => {
    showPopup(".deletePopup");
    setupDeletePopup();
  });

  document.getElementById("select").addEventListener("click", () => {
    window.location.href = "select.html";
  });

  document.getElementById("goBack").addEventListener("click", () => {
    window.location.href = "tables.html";    
  });

  document.getElementById("delete").addEventListener("click", handleDelete);

  document.querySelector(".insertPopup .closeButton").onclick = () => hidePopup(".insertPopup");
  document.querySelector(".deletePopup .closeButton").onclick = () => hidePopup(".deletePopup");

  document.querySelector("#createIndex").addEventListener("click", () => {
    const fieldName = prompt("Enter field name to index:");
    if (fieldName) {
        const isUnique = confirm("Should the index be unique?");
        createIndex(fieldName, isUnique);
    }
  });

  document.querySelector("#dropIndex").addEventListener("click", async () => {
        try {
            const response = await fetch(`http://127.0.0.1:8000/list_indexes?database_name=${databaseName}&table_name=${tableName}`);
            const data = await response.json();
            const indexes = data.indexes;
    
            const indexNames = Object.keys(indexes);
            if (indexNames.length === 0) {
                alert("No indexes to drop.");
                return;
            }
    
            const toDrop = prompt(`Enter the name of the index to drop:`);
    
            if (toDrop && indexNames.includes(toDrop)) {
                await dropIndex(toDrop);
            } else {
                alert("Invalid index name.");
            }
        } catch (err) {
            console.error("Failed to fetch or drop index:", err);
            alert("Error dropping index.");
        }
    });

  // load initial data
  await Promise.all([
    createTableStructure(),
    listIndexes(),
    loadForeignKeys(databaseName, tableName)
  ]);
});
