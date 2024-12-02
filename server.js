const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const xlsx = require('xlsx'); // For handling Excel files
require('dotenv').config(); // For loading environment variables

const app = express();
const port = process.env.PORT || 5000; // Use environment variable or default to 5000

app.use(cors());
app.use(express.json());

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Create a MySQL connection pool
const pool = mysql.createPool({
  connectionLimit: 10, // Set the maximum number of connections in the pool
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
});

// Test the connection
pool.getConnection((err, connection) => {
  if (err) {
    console.error('Database connection error:', err);
    return;
  }
  console.log('Connected to the MySQL database.');
  connection.release(); // Release the connection back to the pool
});

// Route to get all table names
app.get('/tables', (req, res) => {
  pool.query("SHOW TABLES", (err, results) => {
    if (err) {
      console.error('Error fetching table names:', err);
      return res.status(500).send('Error fetching table names');
    }
    const tables = results.map(row => Object.values(row)[0]);
    res.json(tables);
  });
});

// Route to get data from a specific table
app.get('/table-data/:tableName', (req, res) => {
  const tableName = req.params.tableName;

  pool.query(`SELECT * FROM ${mysql.escapeId(tableName)}`, (err, results) => {
    if (err) {
      console.error(`Error fetching data from ${tableName}:`, err);
      return res.status(500).send(`Error fetching data from ${tableName}`);
    }
    res.json(results);
  });
});

// Route to create a new table
app.post('/create-table', (req, res) => {
  const { tableName, columns } = req.body;

  if (!tableName || !columns || !Array.isArray(columns)) {
    return res.status(400).send('Table name and columns are required');
  }

  const columnsDefinition = columns.map(col => `${mysql.escapeId(col.name)} ${col.type}`).join(', ');
  const query = `CREATE TABLE IF NOT EXISTS ${mysql.escapeId(tableName)} (${columnsDefinition})`;

  pool.query(query, (err, results) => {
    if (err) {
      console.error(`Error creating table ${tableName}:`, err);
      return res.status(500).send(`Error creating table ${tableName}`);
    }
    res.status(201).send(`Table ${tableName} created successfully`);
  });
});

// Route to delete a table
app.delete('/delete-table/:tableName', (req, res) => {
  const tableName = req.params.tableName;

  if (!tableName) {
    return res.status(400).send('Table name is required');
  }

  const query = `DROP TABLE IF EXISTS ${mysql.escapeId(tableName)}`;

  pool.query(query, (err, results) => {
    if (err) {
      console.error(`Error deleting table ${tableName}:`, err);
      return res.status(500).send(`Error deleting table ${tableName}`);
    }
    res.send(`Table ${tableName} deleted successfully`);
  });
});

// Route to update data in a table
app.put('/update-table/:tableName', (req, res) => {
  const tableName = req.params.tableName;
  const data = req.body; // Expecting an array of objects

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).send('Invalid data format.');
  }

  pool.getConnection((err, connection) => {
    if (err) return res.status(500).send('Error connecting to the database');

    connection.beginTransaction((err) => {
      if (err) return connection.rollback(() => res.status(500).send('Error starting transaction'));

      let queriesCompleted = 0;

      data.forEach(row => {
        const uniqueColumns = Object.keys(row).filter(col => col !== 'updateField');
        const updateFields = Object.keys(row).filter(col => col === 'updateField');
        
        if (uniqueColumns.length === 0 || updateFields.length === 0) {
          return res.status(400).send('No unique columns or update fields specified.');
        }

        const conditions = uniqueColumns.map(col => `${mysql.escapeId(col)} = ?`).join(' AND ');
        const updates = updateFields.map(col => `${mysql.escapeId(col)} = ?`).join(', ');
        const values = [...uniqueColumns.map(col => row[col]), ...updateFields.map(col => row[col])];
        
        const query = `UPDATE ${mysql.escapeId(tableName)} SET ${updates} WHERE ${conditions}`;
        connection.query(query, values, (err) => {
          if (err) {
            console.error('Error updating row:', err);
            return connection.rollback(() => res.status(500).send('Error updating table data'));
          }

          queriesCompleted++;
          if (queriesCompleted === data.length) {
            connection.commit((err) => {
              if (err) {
                console.error('Error committing transaction:', err);
                return connection.rollback(() => res.status(500).send('Error updating table data'));
              }
              connection.release();
              res.send('Table data updated successfully');
            });
          }
        });
      });
    });
  });
});

// Route to upload data to a table from Excel file
// Route to upload data to a specific table
app.post('/upload/:tableName', async (req, res) => {
  const { tableName } = req.params;
  const { data } = req.body; // This is the parsed CSV data

  if (!data || !data.length) {
      return res.status(400).send('No data to upload.');
  }

  const connection = await pool.getConnection();

  try {
      // Construct the SQL query for inserting data
      const keys = Object.keys(data[0]);
      const values = data.map(row => keys.map(key => row[key]));
      const placeholders = values.map(() => `(${keys.map(() => '?').join(',')})`).join(',');

      const sql = `INSERT INTO ${tableName} (${keys.join(',')}) VALUES ${placeholders}`;
      
      await connection.query(sql, values.flat());
      res.status(200).send('Data uploaded successfully!');
  } catch (error) {
      console.error('Error uploading data:', error);
      res.status(500).send('Failed to upload data.');
  } finally {
      connection.release();
  }
});


// Start the server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
