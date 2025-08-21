const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  host: process.env.PGHOST,
  port: process.env.PGPORT,
  database: process.env.PGDATABASE,
});

exports.getAgents = async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT id, customer_name AS name FROM public.customers WHERE customer_type = 'Agent'"
    );
    console.log('Agents fetched:', result.rows);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching agents:', error.stack);
    res.status(500).json({ error: 'Internal server error', details: error.message });
  }
};