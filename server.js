const express = require('express');
const { Pool } = require('pg');
const cors = require('cors');
const multer = require('multer');
const fs = require('fs');
const xlsx = require('xlsx');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

// Configure multer for file uploads
const upload = multer({ dest: 'uploads/' });

// Create a PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    require: true,
    rejectUnauthorized: false
  }
});

// Test the connection
pool.connect()
  .then(client => {
    console.log('Connected to PostgreSQL database');
    client.release();
  })
  .catch(err => console.error('Database connection error:', err));

// Route to get all table names
app.get('/tables', async (req, res) => {
  try {
    const query = `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
    `;
    const { rows } = await pool.query(query);
    const tables = rows.map(row => row.table_name);
    res.json(tables);
  } catch (err) {
    console.error('Error fetching table names:', err);
    res.status(500).send('Error fetching table names');
  }
});

// Route to get data from a specific table
app.get('/table-data/:tableName', async (req, res) => {
  const tableName = req.params.tableName;
  try {
    const { rows } = await pool.query(
      'SELECT * FROM $1:name',
      [tableName]
    );
    res.json(rows);
  } catch (err) {
    console.error(`Error fetching data from ${tableName}:`, err);
    res.status(500).send(`Error fetching data from ${tableName}`);
  }
});

// Route to create a new table
app.post('/create-table', async (req, res) => {
  const { tableName, columns } = req.body;

  if (!tableName || !columns || !Array.isArray(columns)) {
    return res.status(400).send('Table name and columns are required');
  }

  try {
    const columnsDefinition = columns
      .map(col => `"${col.name}" ${col.type}`)
      .join(', ');
    const query = `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnsDefinition})`;

    await pool.query(query);
    res.status(201).send(`Table ${tableName} created successfully`);
  } catch (err) {
    console.error(`Error creating table ${tableName}:`, err);
    res.status(500).send(`Error creating table ${tableName}`);
  }
});

// Route to delete a table
app.delete('/delete-table/:tableName', async (req, res) => {
  const tableName = req.params.tableName;

  if (!tableName) {
    return res.status(400).send('Table name is required');
  }

  try {
    await pool.query('DROP TABLE IF EXISTS $1:name', [tableName]);
    res.send(`Table ${tableName} deleted successfully`);
  } catch (err) {
    console.error(`Error deleting table ${tableName}:`, err);
    res.status(500).send(`Error deleting table ${tableName}`);
  }
});

// Route to update data in a table
app.put('/update-table/:tableName', async (req, res) => {
  const tableName = req.params.tableName;
  const data = req.body;

  if (!Array.isArray(data) || data.length === 0) {
    return res.status(400).send('Invalid data format.');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    for (const row of data) {
      const uniqueColumns = Object.keys(row).filter(col => col !== 'updateField');
      const updateFields = Object.keys(row).filter(col => col === 'updateField');

      if (uniqueColumns.length === 0 || updateFields.length === 0) {
        throw new Error('No unique columns or update fields specified.');
      }

      const conditions = uniqueColumns.map((col, idx) => `"${col}" = $${idx + 1}`).join(' AND ');
      const updates = updateFields.map((col, idx) =>
        `"${col}" = $${uniqueColumns.length + idx + 1}`
      ).join(', ');

      const values = [
        ...uniqueColumns.map(col => row[col]),
        ...updateFields.map(col => row[col])
      ];

      const query = `
        UPDATE "${tableName}"
        SET ${updates}
        WHERE ${conditions}
      `;

      await client.query(query, values);
    }

    await client.query('COMMIT');
    res.send('Table data updated successfully');
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Error updating table data:', err);
    res.status(500).send('Error updating table data');
  } finally {
    client.release();
  }
});

// Route to upload data to a specific table
app.post('/upload/:tableName', async (req, res) => {
  const { tableName } = req.params;
  const { data } = req.body;

  if (!data || !data.length) {
    return res.status(400).send('No data to upload.');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const keys = Object.keys(data[0]);
    const columns = keys.map(key => `"${key}"`).join(',');

    for (const row of data) {
      const values = keys.map(key => row[key]);
      const placeholders = keys.map((_, idx) => `$${idx + 1}`).join(',');

      const query = `
        INSERT INTO "${tableName}" (${columns})
        VALUES (${placeholders})
      `;

      await client.query(query, values);
    }

    await client.query('COMMIT');
    res.status(200).send('Data uploaded successfully!');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error uploading data:', error);
    res.status(500).send('Failed to upload data.');
  } finally {
    client.release();
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});