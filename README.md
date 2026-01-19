# ❄️ Igloo - Low-Code object management in Snowflake

> **A streamlined, low-code object management solution built entirely inside Snowflake using Streamlit.**

![Status](https://img.shields.io/badge/Status-In%20Development-yellow) ![Snowflake](https://img.shields.io/badge/Built%20on-Snowflake-blue) ![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-red) ![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)



## 📖 Overview

**Igloo** is a low-code GUI that simplifies Snowflake database object management. Instead of writing DDL from scratch, you can visually design and deploy Tables, Views, and Dynamic Tables through an intuitive wizard-style interface. It's perfect for data engineers, analysts, and anyone who wants to work with Snowflake objects without constantly context-switching to SQL editors.


## Why Igloo?

- Visual First: Point-and-click interface for object creation
- Smart Metadata: Auto-fetches schemas, tables, and column definitions
- Preview & Deploy: Review generated DDL before execution
- All-in-Snowflake: Runs as a Snowflake Streamlit app (no external hosting needed)
- Extensible Design: Built with a clean component architecture for easy customization


## Features

### Connection Dashboard
- Real-time **connection status** monitoring.
- Visual display of current **Role**, **Warehouse**, and **Database**.
- Secure credential management via Streamlit secrets.

### Object Builder
A Wizard-style interface to create objects from scratch or based on existing data:
- **Tables:** Define columns, types, and nullability manually.
- **Views:** Select source schemas/tables and apply simple column mappings.
- **Dynamic Tables:** Configure target lag and warehouse settings visually.

### Low-Code Data Editor
- **Interactive Grid:** Add, remove, and modify columns using a spreadsheet-like interface.
- **Smart Type Detection:** Automatically fetches and suggests data types from source tables.

### One-Click Deployment
- Generates production-ready DDL.
- **Preview Mode:** Review the SQL code before deploying.
- **Direct Execution:** Deploys the object to Snowflake with a single click.

---

## Project Structure

```text
Snowfake-Igloo/
├── streamlit_app.py        # Main application entry point (Traffic Controller)
├── components/             # UI Components & Widgets
│   ├── home_ui.py          # Dashboard & Connection Status
│   ├── builders.py         # Object Creation Wizards (Logic + UI)
│   ├── shared_grid.py      # Reusable Data Editor Component
│   └── deploy_ui.py        # SQL Deployment & Execution Button
├── models/                 # Python Classes for Snowflake Objects
│   ├── table.py
│   ├── view.py
│   └── dynamic_table.py
├── utils/                  # Backend Utilities
│   ├── data_provider.py    # Fetches Schemas, Tables & Columns
│   └── snowflake_connector.py # Handles Session & Auth
└── .streamlit/             # Configuration
    ├── config.toml         # Theme & UI Settings
    └── secrets.toml        # Credentials (Not committed)
```

## Configure Secrets
Create a file named `.streamlit/secrets.toml` in the root directory and add your Snowflake credentials:

```toml
[snowflake]
user = "YOUR_USER"
password = "YOUR_PASSWORD"
account = "YOUR_ACCOUNT_IDENTIFIER"
role = "YOUR_ROLE"
warehouse = "YOUR_WAREHOUSE"
database = "YOUR_DATABASE"
schema = "YOUR_SCHEMA"
```


## Future Roadmap

- [X] **Column Transformations:** The option to implement column level transformation (e.g., `LEFT()`).
- [ ] **Join objects:** The opportunity to use join with other objects.
- [X] **Modify Existing Objects:** Load an existing table/view and apply changes.
- [X] **GIT integration:** Implement a version control system.
- [ ] **Orchestration Objects:** UI support for creating `Tasks` and `Streams`.

---


## License

This project is open-source.


## Show Your Support
If you find Igloo helpful, consider giving it a star on GitHub! It helps others discover the project. Also you can [![Ko-fi](https://img.shields.io/badge/Support%20me-on%20Ko--fi-F16061?style=flat&logo=ko-fi&logoColor=white)](https://ko-fi.com/matu09)