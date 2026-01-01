const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const pool = new Pool({
  host: process.env.PGHOST || 'localhost',
  port: process.env.PGPORT || 5432,
  database: process.env.PGDATABASE || 'iapd',
  user: process.env.PGUSER || 'iapdadmin',
  password: process.env.PGPASSWORD || 'AdvPwd#2025',
  ssl: false,
  // Add connection timeout and retry settings
  connectionTimeoutMillis: 5000,
  idleTimeoutMillis: 30000,
  max: 20
});

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('❌ Database connection failed:', err);
  } else {
    console.log('✅ Database connected successfully');
  }
});

// API Routes

// Get risk statistics
app.get('/api/risk-statistics', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        risk_category,
        COUNT(DISTINCT sec_number) as firm_count,
        ROUND(COUNT(DISTINCT sec_number) * 100.0 / SUM(COUNT(DISTINCT sec_number)) OVER (), 2) as percentage,
        ROUND(AVG(score), 2) as avg_score
      FROM ia_risk_score 
      GROUP BY risk_category
      ORDER BY 
        CASE risk_category 
          WHEN 'Critical' THEN 1 
          WHEN 'High' THEN 2 
          WHEN 'Medium' THEN 3 
          WHEN 'Low' THEN 4 
        END
    `);
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching risk statistics:', error);
    res.status(500).json({ error: 'Failed to fetch risk statistics' });
  }
});

// Get firms by risk category
app.get('/api/firms/:riskCategory?', async (req, res) => {
  try {
    const { riskCategory } = req.params;
    const limit = parseInt(req.query.limit) || 100;
    
    let whereClause = '';
    let params = [limit];
    
    if (riskCategory) {
      whereClause = 'WHERE rs.risk_category = $2';
      params = [limit, riskCategory];
    }
    
    const result = await pool.query(`
      SELECT 
        rs.sec_number,
        COALESCE(latest.firm_name::TEXT, 'Unknown') as firm_name,
        rs.score,
        rs.risk_category,
        latest.filing_date as latest_filing_date,
        rs.factors,
        latest.raum,
        latest.client_count,
        latest.account_count
      FROM ia_risk_score rs
      LEFT JOIN (
        SELECT DISTINCT ON (sec_number) 
          sec_number, 
          firm_name, 
          filing_date, 
          raum, 
          client_count, 
          account_count
        FROM ia_filing 
        ORDER BY sec_number, filing_date DESC
      ) latest ON rs.sec_number = latest.sec_number
      ${whereClause}
      ORDER BY rs.score DESC, latest.filing_date DESC
      LIMIT $1
    `, params);
    
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching firms:', error);
    res.status(500).json({ error: 'Failed to fetch firms' });
  }
});

// Get top high-risk firms
app.get('/api/top-risky-firms', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const result = await pool.query(`
      SELECT rs.sec_number, rs.score, rs.risk_category, ic.firm_name, rs.factors
      FROM ia_risk_score rs
      LEFT JOIN ia_filing ic ON rs.sec_number = ic.sec_number AND rs.filing_date = ic.filing_date
      WHERE rs.risk_category IN ('High', 'Critical')
      ORDER BY rs.score DESC, rs.filing_date DESC
      LIMIT $1
    `, [limit]);
    
    res.json(result.rows);
  } catch (error) {
    console.error('Error fetching top risky firms:', error);
    res.status(500).json({ error: 'Failed to fetch top risky firms' });
  }
});

// Get dashboard summary
app.get('/api/dashboard-summary', async (req, res) => {
  try {
    // Get risk statistics
    const statsResult = await pool.query(`
      SELECT 
        risk_category,
        COUNT(DISTINCT sec_number) as firm_count,
        ROUND(COUNT(DISTINCT sec_number) * 100.0 / SUM(COUNT(DISTINCT sec_number)) OVER (), 2) as percentage,
        ROUND(AVG(score), 2) as avg_score
      FROM ia_risk_score 
      GROUP BY risk_category
      ORDER BY 
        CASE risk_category 
          WHEN 'Critical' THEN 1 
          WHEN 'High' THEN 2 
          WHEN 'Medium' THEN 3 
          WHEN 'Low' THEN 4 
        END
    `);
    
    // Get total firms count
    const totalResult = await pool.query('SELECT COUNT(DISTINCT sec_number) as total FROM ia_risk_score');
    
    // Get recent filings count
    const recentResult = await pool.query(`
      SELECT COUNT(*) as recent_filings 
      FROM ia_filing 
      WHERE filing_date >= CURRENT_DATE - INTERVAL '30 days'
    `);
    
    res.json({
      riskStatistics: statsResult.rows,
      totalFirms: parseInt(totalResult.rows[0].total),
      recentFilings: parseInt(recentResult.rows[0].recent_filings)
    });
  } catch (error) {
    console.error('Error fetching dashboard summary:', error);
    res.status(500).json({ error: 'Failed to fetch dashboard summary' });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 API server running on port ${PORT}`);
  console.log(`📊 Risk data API available at http://localhost:${PORT}/api`);
}); 