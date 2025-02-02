# Infra_dbt_sql

Welcome to the **Infra_dbt_sql** project! This repository is designed to streamline data transformation processes using [dbt (data build tool)](https://www.getdbt.com/). It contains SQL models and schema configurations that define and document the transformations applied to your data.

## 📂 Project Structure

The repository is organized as follows:

- **`models/`**: Contains SQL files that define your dbt models. Each file represents a transformation applied to raw data.
- **`models/schema.yml`**: Defines tests and documentation for models, ensuring data quality.
- **`dbt_project.yml`**: Configures project-level settings for dbt, including models, materializations, and database connections.

## 🚀 Getting Started

### 1️⃣ Clone the Repository
git clone https://github.com/Jigna14/Infra_dbt_sql.git
cd Infra_dbt_sql

2️⃣ Install dbt
Ensure you have dbt installed. Follow the official dbt installation guide based on your operating system.

3️⃣ Configure Your Profile
Set up your profiles.yml file to connect dbt to your data warehouse. Refer to the dbt profile configuration guide for detailed instructions.

4️⃣ Run dbt Commands
Execute the following dbt commands to test and deploy transformations:

Compile models:
dbt compile

Run models:
dbt run

Test models:
dbt test

Generate documentation:
dbt docs generate

Serve documentation:
dbt docs serve


🛠 Models & Transformations
The models/ directory contains SQL files that define data transformations. Some key models include:

stg_*.sql: Staging models used to clean and prepare raw data.
int_*.sql: Intermediate models applying business logic.
final_*.sql: Final models ready for reporting and analytics.
✅ Data Quality & Testing
The schema.yml file defines tests to ensure data integrity. Some commonly used tests include:

not_null: Ensures a column has no null values.
unique: Ensures values in a column are unique.
relationships: Validates foreign key relationships between tables.
To run these tests, use:
dbt test

📖 Documentation
To generate and serve interactive documentation, run:
dbt docs generate
dbt docs serve

This will launch a web-based UI displaying model dependencies, columns, and test coverage.

📌 Contributing
We welcome contributions! To contribute:

Fork the repository.
Create a feature branch.
Make changes and commit with descriptive messages.
Open a pull request for review.
📄 License
This project is licensed under the MIT License. See the LICENSE file for more details.
