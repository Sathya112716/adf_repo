# Silver Layer ETL Process

## 1. Customer File
- Convert column headers to snake case using a UDF function.
- Split the "Name" column into "first_name" and "last_name".
- Extract domain from the "Email" column.
- Create a "gender" column based on the values in the dataset.
- Split "Joining date" into "date" and "time" columns.
- Format the "date" column to "yyyy-MM-dd".
- Create an "expenditure-status" column based on the "spent" column.
- Upsert the data into the "customer" table in the silver layer.

## 2. Product File
- Convert column headers to snake case using a UDF function.
- Create a "sub_category" column based on the "Category" column.
- Upsert the data into the "product" table in the silver layer.

## 3. Store
- Convert column headers to snake case using a UDF function.
- Extract "store category" from the "Email" column.
- Format "created_at" and "updated_at" columns to "yyyy-MM-dd".
- Upsert the data into the "store" table in the silver layer.

## 4. Sales
- Convert column headers to snake case using a UDF function.
- Upsert the data into the "customer_sales" table in the silver layer.

# Gold Layer Processing

## 1. StoreProductSalesAnalysis Table
- Read data from product and store tables.
- Extract necessary columns for analysis.
- Read data from customer_sales table.
- Extract necessary columns for analysis.
- Merge data from product, store, and customer_sales tables.
- Write data into the "StoreProductSalesAnalysis" table in the gold layer.
- Use "overwrite" mode to update the table.

# Instructions for Execution

- Ensure all necessary dependencies are installed.
- Run each ETL process in the specified order.
- Verify data integrity after each step.
- Commit changes to the appropriate branch in the Git repository.
- Update documentation and notify stakeholders of updates.
