const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: process.env.PGHOST || 'localhost',
  port: process.env.PGPORT || 5432,
  database: process.env.PGDATABASE || 'iapd',
  user: process.env.PGUSER || 'iapdadmin',
  password: process.env.PGPASSWORD || 'AdvPwd#2025',
  ssl: false
});

async function testConnection() {
  try {
    console.log('Testing database connection...');
    console.log('Host:', process.env.PGHOST || 'localhost');
    console.log('Port:', process.env.PGPORT || 5432);
    console.log('Database:', process.env.PGDATABASE || 'iapd');
    console.log('User:', process.env.PGUSER || 'iapdadmin');
    
    const result = await pool.query('SELECT NOW() as current_time');
    console.log('✅ Connection successful:', result.rows[0]);
    
    const countResult = await pool.query('SELECT COUNT(*) as total FROM ia_risk_score');
    console.log('✅ Risk scores count:', countResult.rows[0].total);
    
    await pool.end();
  } catch (error) {
    console.error('❌ Connection failed:', error.message);
    await pool.end();
  }
}

testConnection(); 