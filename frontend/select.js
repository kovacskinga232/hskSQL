const operators = ["=", ">", "<", ">=", "<=", "!="];
let columns = [];
let tableNames = [];

let fullResults = [];
let currentPage = 1;
const rowsPerPage = 10;

window.onload = async () => {
  const table = localStorage.getItem("selectedTable");
  const database = localStorage.getItem("dbName");

  if (!table || !database) {
    alert("Missing database or table selection.");
    window.location.href = "table_details.html";
    return;
  }

  document.getElementById("tableTitle").textContent = `FROM ${table}`;

  try {
    const res = await fetch(`http://127.0.0.1:8000/get_columns?database_name=${database}&table_name=${table}`);
    const data = await res.json();
    columns = Object.keys(data.columns);

    const result = await fetch(`http://127.0.0.1:8000/get_database?database_name=${encodeURIComponent(database)}`);
    const dbData = await result.json();
    tableNames = Object.keys(dbData.tables);

    addCondition();
    addSelect();
    addJoin(tableNames);
  } catch (err) {
    console.error("Failed to load columns:", err);
    alert("Could not fetch table schema.");
  }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SELECT
function addSelect() {
  const container = document.getElementById("columnTextboxes");
  container.innerHTML = "";

  // Add + button
  const addBtn = document.createElement("button");
  addBtn.className = "selectAddButton";
  addBtn.textContent = "+";
  addBtn.type = "button";

  addBtn.onclick = () => {
  const lastRow = container.querySelector(".select-row:last-child");
  if (lastRow) {
    const input = lastRow.querySelector(".column-input").value.trim();
    if (!input) {
      alert("Please fill the last select column before adding a new one.");
      return;
    }
  }
  addSelectRow(container);
};

  container.appendChild(addBtn);

  // Add first row by default
  addSelectRow(container);
}

function addSelectRow(container) {
  const row = document.createElement("div");
  row.className = "select-row";
  row.style.opacity = 0; // start transparent

  row.innerHTML = `
    <select class="select-agg">
      <option value="">(none)</option>
      <option value="MIN">MIN</option>
      <option value="MAX">MAX</option>
      <option value="AVG">AVG</option>
      <option value="COUNT">COUNT</option>
      <option value="SUM">SUM</option>
    </select>
    <input type="text" class="column-input" placeholder="table.column" list="columns" />
    <button type="button" class="sb" >Remove</button>
  `;

  row.querySelector("button").onclick = () => {
    if (confirm("Remove this select column?")) row.remove();
  };

  container.appendChild(row);

  // Animate opacity to 1
  requestAnimationFrame(() => {
    row.style.transition = "opacity 0.3s ease";
    row.style.opacity = 1;
  });
}

function addDropDown(container) {
  const div = document.createElement("div");
  div.className = "dpr";

  div.innerHTML = `
    <input type="checkbox" class="columnS" checked />
    <select class="join-column">
      ${tableNames.map(tbl => `<option value="${tbl}">${tbl}</option>`).join("")}
    </select>
    <button type="button" onclick="this.parentNode.remove()">Remove</button>
  `;

  container.appendChild(div);
}

function addCondition() {
  const container = document.getElementById("conditionContainer");

  const row = document.createElement("div");
  row.className = "condition-row";

  row.innerHTML = `
    <input type="checkbox" class="use-condition" checked />
    <input type="text" class="column" placeholder="table.column" list="columns" />
    <select class="operator">
      ${operators.map(op => `<option value="${op}">${op}</option>`).join("")}
    </select>
    <input type="text" class="value" placeholder="Value" />
    <button type="button" onclick="this.parentNode.remove()">Remove</button>
  `;

  container.appendChild(row);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// JOIN
function addJoin(tables) {
  const container = document.getElementById("joinTables");

  const row = document.createElement("div");
  row.className = "join-row";

  row.innerHTML = `
    <input type="checkbox" class="use-join" checked />
    <select class="join-column">
      ${tables.map(tbl => `<option value="${tbl}">${tbl}</option>`).join("")}
    </select>
    <button type="button" onclick="this.parentNode.remove()">Remove</button>
  `;

  container.appendChild(row);
}

// GROUP BY
function addGroupBy() {
  const container = document.getElementById("groupByContainer");

  const row = document.createElement("div");
  row.className = "groupby-row";

  row.innerHTML = `
    <input type="text" class="groupby-input column-input" placeholder="table.column" list="columns" />
    <button type="button" class="btn-remove">Remove</button>
  `;

  container.appendChild(row);
}

function getSelectedColumns() {
  const rows = document.querySelectorAll("#columnTextboxes .select-row");
  const columns = [];

  const mainTable = localStorage.getItem("selectedTable") || "";
  const joinedTables = document.querySelectorAll(".join-row");
  const isJoinedQuery = joinedTables.length > 0;

  rows.forEach(row => {
    const func = row.querySelector(".select-agg").value.trim();
    const col = row.querySelector(".column-input").value.trim();

    if (!col) return;

    if (col === "*") {
      if (func) {
        columns.push(`${func}(*)`); // e.g. COUNT(*)
      } else {
        columns.push("*");
      }
    } else {
      if (func) {
        // Aggregation with explicit column
        if (col.includes(".")) {
          columns.push(`${func}(${col})`);
        } else {
          columns.push(isJoinedQuery ? `${func}(${mainTable}.${col})` : `${func}(${col})`);
        }
      } else {
        // Regular column selection
        if (col.includes(".")) {
          columns.push(col);
        } else {
          columns.push(isJoinedQuery ? `${mainTable}.${col}` : col);
        }
      }
    }
  });

  return columns;
}

function getGroupByColumns() {
  const inputs = document.querySelectorAll(".groupby-input");
  return [...inputs]
    .map(input => input.value.trim())
    .filter(value => value !== "");
}

// Add this function to handle ORDER BY additions
function addOrderBy() {
  const container = document.getElementById("orderByContainer");

  const row = document.createElement("div");
  row.className = "orderby-row";

  row.innerHTML = `
    <input type="text" class="orderby-input" placeholder="table.column" list="columns" />
    <select class="orderby-direction">
      <option value="ASC">ASC</option>
      <option value="DESC">DESC</option>
    </select>
    <button type="button" class="obb" "onclick="this.parentNode.remove()">Remove</button>
  `;

  container.appendChild(row);
}

// Add this function to get ORDER BY columns
function getOrderByColumns() {
  const rows = document.querySelectorAll(".orderby-row");
  const orderBy = [];
  
  rows.forEach(row => {
    const column = row.querySelector(".orderby-input").value.trim();
    const direction = row.querySelector(".orderby-direction").value;
    
    if (column) {
      orderBy.push(`${column}, ${direction}`);
    }
  });
  
  return orderBy.length > 0 ? orderBy.join(", ") : null;
}

// Update the submitQuery function to include ORDER BY
async function submitQuery() {
  const table = localStorage.getItem("selectedTable");
  const database = localStorage.getItem("dbName");

  const selectedColumns = getSelectedColumns();
  if (selectedColumns.length === 0) {
    alert("Please select at least one column to display.");
    return;
  }

  const rows = document.querySelectorAll(".condition-row");
  const conditions = [];

  rows.forEach(row => {
    const use = row.querySelector(".use-condition").checked;
    if (!use) return;

    const column = row.querySelector(".column").value;
    const operator = row.querySelector(".operator").value;
    const value = row.querySelector(".value").value;

    if (column && operator && value) {
      conditions.push({ column, operator, value });
    }
  });

  // join
  const joinedTables = document.querySelectorAll(".join-row");
  const tables = [table];

  joinedTables.forEach(row => {
    const use = row.querySelector(".use-join").checked;
    if (!use) return;

    const value = row.querySelector(".join-column")?.value;

    if (value && !tables.includes(value)) {
      tables.push(value);
    }
  });

  // Prepare query parameters
  const params = new URLSearchParams();
  params.append('database_name', database);
  params.append('table_name', JSON.stringify(tables));
  params.append('columns', JSON.stringify(selectedColumns));

  const groupByColumns = getGroupByColumns();
  if (groupByColumns.length > 0) {
    params.append('group_by', JSON.stringify(groupByColumns));
  }

  const orderBy = getOrderByColumns();
  if (orderBy) {
    params.append('order_by', orderBy);
  }
  
  if (conditions.length > 0) {
    params.append('conditions', JSON.stringify(conditions));
  }

  const url = `http://127.0.0.1:8000/execute_select?${params.toString()}`;

  try {
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }
    const data = await res.json();
    console.log("Query results:", data);

    displayResults(data);
  } catch (err) {
    console.error("Query failed:", err);
    alert(`Query failed: ${err.message}`);
  }
}

function renderPage(page, visibleColumns = null) {
  const container = document.getElementById("queryResults");
  container.innerHTML = "";

  if (!fullResults || fullResults.length === 0) {
    container.textContent = "No results found.";
    return;
  }

  const table = document.createElement("table");
  const header = document.createElement("tr");

  // Use visibleColumns if provided, otherwise all keys
  const keys = visibleColumns || Object.keys(fullResults[0]);

  keys.forEach(key => {
    const th = document.createElement("th");
    th.textContent = key;
    header.appendChild(th);
  });

  table.appendChild(header);

  const startIndex = (page - 1) * rowsPerPage;
  const endIndex = Math.min(startIndex + rowsPerPage, fullResults.length);
  const pageResults = fullResults.slice(startIndex, endIndex);

  pageResults.forEach(row => {
    const tr = document.createElement("tr");
    keys.forEach(key => {
      const td = document.createElement("td");
      td.textContent = row[key] !== undefined && row[key] !== null ? row[key].toString() : "NULL";
      tr.appendChild(td);
    });
    table.appendChild(tr);
  });

  container.appendChild(table);
}

function createPaginationControls() {
  const container = document.getElementById("pagination");
  const paginationContainer = document.createElement("div");
  paginationContainer.className = "pagination";

  const totalPages = Math.ceil(fullResults.length / rowsPerPage);

  // Remove old pagination
  const existingPagination = document.querySelector(".pagination");
  if (existingPagination) {
    existingPagination.remove();
  }

  // Previous button
  const prevBtn = document.createElement("button");
  prevBtn.textContent = "«";
  prevBtn.disabled = currentPage === 1;
  prevBtn.onclick = () => {
    if (currentPage > 1) {
      currentPage--;
      renderPage(currentPage);
      createPaginationControls();
    }
  };
  paginationContainer.appendChild(prevBtn);

  const info = document.createElement("span");
  info.textContent = ` Page ${currentPage} of ${totalPages} `;
  paginationContainer.appendChild(info);

  // Next button
  const nextBtn = document.createElement("button");
  nextBtn.textContent = "»";
  nextBtn.disabled = currentPage === totalPages;
  nextBtn.onclick = () => {
    if (currentPage < totalPages) {
      currentPage++;
      renderPage(currentPage);
      createPaginationControls();
    }
  };
  paginationContainer.appendChild(nextBtn);

  container.appendChild(paginationContainer);
}

function displayResults(results) {
  if (!Array.isArray(results)) {
    console.error("Expected array but got:", results);
    fullResults = [];
  } else {
    fullResults = results;
  }

  currentPage = 1;
  renderPage(currentPage);
  createPaginationControls();
}
