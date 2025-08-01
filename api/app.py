#!/usr/bin/env python3
"""
FastAPI backend for IAPD Risk Scoring Dashboard

This API serves risk scoring data and statistics to the frontend dashboard.
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
import os
import sys
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = FastAPI(
    title="IAPD Risk Scoring API",
    description="API for Investment Adviser Public Disclosure risk scoring data",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
def get_db_engine():
    """Create database connection."""
    required_vars = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    for var in required_vars:
        if not os.getenv(var):
            raise HTTPException(status_code=500, detail=f"Missing environment variable: {var}")
    
    host = os.environ["PGHOST"]
    port = os.getenv("PGPORT", "5432")
    user = os.environ["PGUSER"]
    pwd = os.environ["PGPASSWORD"]
    db = os.environ["PGDATABASE"]
    
    dsn = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return sa.create_engine(dsn, pool_pre_ping=True, connect_args={"sslmode": "require"})

# Pydantic models for API responses
from pydantic import BaseModel
from typing import Optional, List

class RiskScore(BaseModel):
    crd: int
    overall_risk_score: float
    risk_category: str
    disclosure_risk: float
    aum_volatility_risk: float
    client_concentration_risk: float
    filing_compliance_risk: float
    cco_stability_risk: float
    size_factor_risk: float
    last_calculation_date: datetime
    risk_factors: Dict[str, Any]

class FirmData(BaseModel):
    crd: int
    firm_name: Optional[str]
    aum: Optional[float]
    total_clients: Optional[int]
    total_accounts: Optional[int]
    last_filing_date: Optional[datetime]
    risk_score: Optional[RiskScore]

class DashboardStats(BaseModel):
    total_firms: int
    new_disclosures: int
    high_severity_alerts: int
    total_aum: float
    firms_with_recent_activity: int
    risk_distribution: Dict[str, int]

class PaginatedResponse(BaseModel):
    data: List[FirmData]
    total: int
    page: int
    page_size: int
    total_pages: int

@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "IAPD Risk Scoring API", "status": "healthy"}

@app.get("/health")
async def health_check():
    """Detailed health check."""
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

@app.get("/api/stats", response_model=DashboardStats)
async def get_dashboard_stats():
    """Get dashboard statistics."""
    try:
        engine = get_db_engine()
        
        with engine.connect() as conn:
            # Get total firms
            result = conn.execute(text("SELECT COUNT(DISTINCT crd) FROM ia_filing"))
            total_firms = result.scalar()
            
            # Get total AUM
            result = conn.execute(text("""
                SELECT SUM(raum) FROM ia_filing 
                WHERE filing_date = (SELECT MAX(filing_date) FROM ia_filing)
            """))
            total_aum = result.scalar() or 0
            
            # Get firms with recent activity (last 30 days)
            thirty_days_ago = datetime.now() - timedelta(days=30)
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT crd) FROM ia_filing 
                WHERE filing_date >= :date
            """), {"date": thirty_days_ago})
            firms_with_recent_activity = result.scalar()
            
            # Get risk distribution using the SQL function
            result = conn.execute(text("SELECT * FROM get_risk_statistics()"))
            risk_stats = result.fetchall()
            risk_distribution = {row.risk_category: row.firm_count for row in risk_stats}
            
            # Get new disclosures (firms with disclosure_flag = 'Y' in last 30 days)
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT crd) FROM ia_filing 
                WHERE filing_date >= :date AND disclosure_flag = 'Y'
            """), {"date": thirty_days_ago})
            new_disclosures = result.scalar()
            
            # Get high severity alerts (Critical and High risk firms)
            result = conn.execute(text("""
                SELECT COUNT(*) FROM risk_scores 
                WHERE risk_category IN ('Critical', 'High')
            """))
            high_severity_alerts = result.scalar()
        
        return DashboardStats(
            total_firms=total_firms,
            new_disclosures=new_disclosures,
            high_severity_alerts=high_severity_alerts,
            total_aum=total_aum,
            firms_with_recent_activity=firms_with_recent_activity,
            risk_distribution=risk_distribution
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@app.get("/api/firms", response_model=PaginatedResponse)
async def get_firms(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    search: Optional[str] = Query(None, description="Search term for firm name or CRD"),
    risk_category: Optional[str] = Query(None, description="Filter by risk category"),
    min_aum: Optional[float] = Query(None, description="Minimum AUM"),
    max_aum: Optional[float] = Query(None, description="Maximum AUM"),
    sort_by: str = Query("crd", description="Sort field"),
    sort_order: str = Query("asc", description="Sort order (asc/desc)")
):
    """Get paginated list of firms with risk scores."""
    try:
        engine = get_db_engine()
        
        # Build query
        base_query = """
            SELECT 
                f.crd,
                f.raum as aum,
                f.total_clients,
                f.total_accounts,
                f.filing_date as last_filing_date,
                rs.overall_risk_score,
                rs.risk_category,
                rs.disclosure_risk,
                rs.aum_volatility_risk,
                rs.client_concentration_risk,
                rs.filing_compliance_risk,
                rs.cco_stability_risk,
                rs.size_factor_risk,
                rs.last_calculation_date,
                rs.risk_factors
            FROM ia_filing f
            LEFT JOIN risk_scores rs ON f.crd = rs.crd
            WHERE f.filing_date = (SELECT MAX(filing_date) FROM ia_filing WHERE crd = f.crd)
        """
        
        params = {}
        conditions = []
        
        if search:
            conditions.append("(f.crd::text LIKE :search OR rs.firm_name LIKE :search)")
            params["search"] = f"%{search}%"
        
        if risk_category:
            conditions.append("rs.risk_category = :risk_category")
            params["risk_category"] = risk_category
        
        if min_aum is not None:
            conditions.append("f.raum >= :min_aum")
            params["min_aum"] = min_aum
        
        if max_aum is not None:
            conditions.append("f.raum <= :max_aum")
            params["max_aum"] = max_aum
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        # Count total
        count_query = f"SELECT COUNT(*) FROM ({base_query}) as subquery"
        with engine.connect() as conn:
            result = conn.execute(text(count_query), params)
            total = result.scalar()
        
        # Add sorting and pagination
        sort_field = sort_by if sort_by in ["crd", "aum", "overall_risk_score", "risk_category"] else "crd"
        sort_direction = "DESC" if sort_order.lower() == "desc" else "ASC"
        
        query = f"""
            {base_query}
            ORDER BY {sort_field} {sort_direction}
            LIMIT :limit OFFSET :offset
        """
        
        params["limit"] = page_size
        params["offset"] = (page - 1) * page_size
        
        # Execute query
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
        
        # Convert to response format
        firms = []
        for row in rows:
            risk_score = None
            if row.overall_risk_score is not None:
                risk_score = RiskScore(
                    crd=row.crd,
                    overall_risk_score=float(row.overall_risk_score),
                    risk_category=row.risk_category,
                    disclosure_risk=float(row.disclosure_risk),
                    aum_volatility_risk=float(row.aum_volatility_risk),
                    client_concentration_risk=float(row.client_concentration_risk),
                    filing_compliance_risk=float(row.filing_compliance_risk),
                    cco_stability_risk=float(row.cco_stability_risk),
                    size_factor_risk=float(row.size_factor_risk),
                    last_calculation_date=row.last_calculation_date,
                    risk_factors=json.loads(row.risk_factors) if row.risk_factors else {}
                )
            
            firm = FirmData(
                crd=row.crd,
                firm_name=None,  # Not available in current schema
                aum=float(row.aum) if row.aum else None,
                total_clients=row.total_clients,
                total_accounts=row.total_accounts,
                last_filing_date=row.last_filing_date,
                risk_score=risk_score
            )
            firms.append(firm)
        
        total_pages = (total + page_size - 1) // page_size
        
        return PaginatedResponse(
            data=firms,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get firms: {str(e)}")

@app.get("/api/firms/{crd}", response_model=FirmData)
async def get_firm_detail(crd: int):
    """Get detailed information for a specific firm."""
    try:
        engine = get_db_engine()
        
        query = """
            SELECT 
                f.crd,
                f.raum as aum,
                f.total_clients,
                f.total_accounts,
                f.filing_date as last_filing_date,
                rs.overall_risk_score,
                rs.risk_category,
                rs.disclosure_risk,
                rs.aum_volatility_risk,
                rs.client_concentration_risk,
                rs.filing_compliance_risk,
                rs.cco_stability_risk,
                rs.size_factor_risk,
                rs.last_calculation_date,
                rs.risk_factors
            FROM ia_filing f
            LEFT JOIN risk_scores rs ON f.crd = rs.crd
            WHERE f.crd = :crd
            ORDER BY f.filing_date DESC
            LIMIT 1
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"crd": crd})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Firm not found")
            
            risk_score = None
            if row.overall_risk_score is not None:
                risk_score = RiskScore(
                    crd=row.crd,
                    overall_risk_score=float(row.overall_risk_score),
                    risk_category=row.risk_category,
                    disclosure_risk=float(row.disclosure_risk),
                    aum_volatility_risk=float(row.aum_volatility_risk),
                    client_concentration_risk=float(row.client_concentration_risk),
                    filing_compliance_risk=float(row.filing_compliance_risk),
                    cco_stability_risk=float(row.cco_stability_risk),
                    size_factor_risk=float(row.size_factor_risk),
                    last_calculation_date=row.last_calculation_date,
                    risk_factors=json.loads(row.risk_factors) if row.risk_factors else {}
                )
            
            return FirmData(
                crd=row.crd,
                firm_name=None,
                aum=float(row.aum) if row.aum else None,
                total_clients=row.total_clients,
                total_accounts=row.total_accounts,
                last_filing_date=row.last_filing_date,
                risk_score=risk_score
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get firm details: {str(e)}")

@app.get("/api/risk-categories")
async def get_risk_categories():
    """Get list of available risk categories."""
    return {
        "categories": ["Low", "Medium", "High", "Critical"],
        "descriptions": {
            "Low": "Minimal risk indicators",
            "Medium": "Some risk indicators present",
            "High": "Multiple risk indicators",
            "Critical": "Severe risk indicators requiring immediate attention"
        }
    }

@app.get("/api/analytics/risk-trends")
async def get_risk_trends(days: int = Query(30, ge=1, le=365, description="Number of days to analyze")):
    """Get risk trend analytics over time."""
    try:
        engine = get_db_engine()
        
        start_date = datetime.now() - timedelta(days=days)
        
        query = """
            SELECT 
                DATE(f.filing_date) as date,
                rs.risk_category,
                COUNT(*) as count
            FROM ia_filing f
            LEFT JOIN risk_scores rs ON f.crd = rs.crd
            WHERE f.filing_date >= :start_date
            GROUP BY DATE(f.filing_date), rs.risk_category
            ORDER BY date, rs.risk_category
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"start_date": start_date})
            rows = result.fetchall()
        
        # Process results into time series
        trends = {}
        for row in rows:
            date_str = row.date.isoformat()
            if date_str not in trends:
                trends[date_str] = {"Low": 0, "Medium": 0, "High": 0, "Critical": 0}
            trends[date_str][row.risk_category] = row.count
        
        return {
            "period_days": days,
            "start_date": start_date.isoformat(),
            "end_date": datetime.now().isoformat(),
            "trends": trends
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get risk trends: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 