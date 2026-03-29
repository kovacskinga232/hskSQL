const API_URL = "http://127.0.0.1:8000";

window.onload = loadOptions;

// load the name of the available databases
async function loadOptions() {
    try {
        const response = await fetch(`${API_URL}/list_databases`);
        const data = await response.json();
        const select = document.getElementById('select');

        // clear all existing options
        while (select.firstChild) {
            select.removeChild(select.firstChild);
        }

        // add default disabled option
        const defaultOption = document.createElement('option');
        defaultOption.value = '';
        defaultOption.textContent = '-- Select a database --';
        defaultOption.disabled = true;
        defaultOption.selected = true;
        select.appendChild(defaultOption);

        // add fetched database options
        Object.keys(data).forEach(name => {
            const option = document.createElement('option');
            option.value = name;
            option.textContent = name;
            select.appendChild(option);
        });
    } catch (err) {
        console.error("Failed to load databases:", err);
    }
}

// POPUPS
const showPopup = (selector) => document.querySelector(selector).classList.add("active");
const hidePopup = (selector, inputId) => {
    document.querySelector(selector).classList.remove("active");
    document.getElementById(inputId).value = "";
};

document.getElementById("createButton").onclick = () => showPopup(".createPopup");
document.querySelector(".createPopup .closeButton").onclick = () => hidePopup(".createPopup", "dbName");

document.getElementById("dropButton").onclick = () => showPopup(".dropPopup");
document.querySelector(".dropPopup .closeButton").onclick = () => hidePopup(".dropPopup", "dropDatabaseName");

// create database
document.getElementById("create").onclick = async () => {
    const name = document.getElementById("dbName").value;

    if (!name) {
        return alert("Please enter a database name!");
    }

    const res = await fetch(`${API_URL}/create_database`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name })
    });

    const result = await res.json();
    alert(result.message || result.detail);
    hidePopup(".createPopup", "dbName");
    loadOptions();
};

// drop database
document.getElementById("drop").onclick = async () => {
    const name = document.getElementById("dropDatabaseName").value;
    const res = await fetch(`${API_URL}/drop_database`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name })
    });

    const result = await res.json();
    alert(result.message || result.detail);
    hidePopup(".dropPopup", "dropDatabaseName");
    loadOptions();
};

// handle database selection
document.getElementById("select").onchange = function () {
    const dbName = this.value;

    if (dbName) {
        localStorage.setItem("dbName", dbName);
        window.location.href = "tables.html?database=" + encodeURIComponent(dbName);
    }
};
