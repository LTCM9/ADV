# IAPD Risk Scoring System

A comprehensive system for scraping SEC investment adviser data, calculating risk scores, and providing a modern dashboard for monitoring regulatory compliance.

## 🎯 Overview

This project automates the collection and analysis of Investment Adviser Public Disclosure (IAPD) data from the SEC website. It provides:

- **Automated Data Collection**: Scrapes SEC website for registered investment adviser filings
- **Risk Scoring Algorithm**: Calculates comprehensive risk scores based on multiple factors
- **Modern Dashboard**: React-based frontend for monitoring and analysis
- **Cloud Infrastructure**: AWS-based deployment with S3, RDS, and EC2
- **API Backend**: FastAPI service for data access and analytics

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SEC Website   │───▶│  Data Pipeline  │───▶│  PostgreSQL DB  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │                        │
                              ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   S3 Storage    │    │  Risk Scoring   │
                       └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │  FastAPI Backend│◄───│  React Frontend │
                       └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Node.js 18+
- PostgreSQL 15+
- AWS Account (for cloud deployment)
- Terraform (for infrastructure)

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd iapd-risk-scoring
   ```

2. **Set up Python environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your database and AWS credentials
   ```

4. **Set up database**
   ```bash
   # Create PostgreSQL database
   createdb iapd
   
   # Run initial data load (optional - for testing)
   python scripts/fetch_iapd_data.py
   python scripts/unzip_iapd_zips.py
   python scripts/load_iapd_to_postgres.py data/unzipped/iapd/
   ```

5. **Start the API server**
   ```bash
   cd api
   python app.py
   ```

6. **Start the frontend**
   ```bash
   npm install
   npm run dev
   ```

## 📊 Risk Scoring Algorithm

The system uses a hybrid approach combining SQL procedures for performance with Python orchestration:

### SQL-Based Risk Calculation (Primary Method)
Located in `scripts/risk_score_procedure.sql`, this approach provides:
- **High Performance**: Direct database execution for large datasets
- **Real-time Updates**: Can be triggered via database events or scheduled jobs
- **Atomic Operations**: Entire calculation runs as a single transaction

**Core Risk Factors:**
1. **Disciplinary Actions (40 points)** - New regulatory disclosures
2. **AUM Drops (25-40 points)** - Significant asset declines (15%+)
3. **Client Losses (8-23 points)** - Client count reductions (15%+)
4. **Account Losses (5-15 points)** - Account count reductions (15%+)
5. **CCO Changes (10 points)** - Chief Compliance Officer turnover
6. **Trend Analysis (10 points)** - Long-term AUM decline patterns

**Additional Risk Factors:**
7. **Leadership Instability (10 points)** - ≥2 director/owner moves in 12 months
8. **New Firm Risk (5 points)** - Adviser < 3 years old
9. **Small Firm Risk (5 points)** - RAUM < $50M

### Python-Based Risk Calculation (Alternative)
Located in `scripts/calculate_risk_scores.py`, this approach provides:
- **Advanced Analytics**: Statistical analysis and machine learning capabilities
- **Complex Algorithms**: Multi-factor weighted scoring with normalization
- **Extensibility**: Easy to add new risk factors and validation

### Risk Categories
- **Low Risk (0-29 points)**: Minimal concerns
- **Medium Risk (30-59 points)**: Moderate concerns requiring monitoring
- **High Risk (60-79 points)**: Significant concerns requiring attention
- **Critical Risk (80+ points)**: Immediate attention required

## 🛠️ Data Pipeline

### 1. Data Collection (`fetch_iapd_data.py`)
- Scrapes SEC website for ZIP archives
- Downloads registered investment adviser data
- Excludes exempt reporting advisers
- Implements rate limiting and error handling

### 2. Data Extraction (`unzip_iapd_zips.py`)
- Extracts Excel and CSV files from ZIP archives
- Handles various file formats and encodings
- Skips corrupted or invalid files

### 3. Data Loading (`load_iapd_to_postgres.py`)
- Processes Excel/CSV files into PostgreSQL
- Normalizes data schemas
- Handles S3 integration for cloud storage
- Supports multi-threaded processing

### 4. Risk Calculation (Hybrid Approach)
- **Primary**: SQL procedure (`scripts/risk_score_procedure.sql`) for high-performance calculation
- **Alternative**: Python script (`scripts/calculate_risk_scores.py`) for advanced analytics
- **Orchestration**: Python wrapper (`scripts/run_risk_calculation.py`) for execution and monitoring
- Updates `ia_risk_score` table with comprehensive risk factors

## 🌐 API Endpoints

### Core Endpoints
- `GET /api/stats` - Dashboard statistics
- `GET /api/firms` - Paginated firm list with filtering
- `GET /api/firms/{crd}` - Individual firm details
- `GET /api/risk-categories` - Available risk categories

### Analytics Endpoints
- `GET /api/analytics/risk-trends` - Risk trend analysis
- `GET /health` - System health check

### Query Parameters
- `page` - Page number for pagination
- `page_size` - Items per page (1-100)
- `search` - Search by firm name or CRD
- `risk_category` - Filter by risk level
- `min_aum` / `max_aum` - AUM range filtering
- `sort_by` - Sort field (crd, aum, overall_risk_score, risk_category)
- `sort_order` - Sort direction (asc/desc)

## ☁️ AWS Infrastructure

### Terraform Deployment

1. **Initialize Terraform**
   ```bash
   cd infrastructure
   terraform init
   ```

2. **Configure variables**
   ```bash
   # Create terraform.tfvars
   aws_region = "us-east-1"
   environment = "development"
   db_password = "your-secure-password"
   ```

3. **Deploy infrastructure**
   ```bash
   terraform plan
   terraform apply
   ```

### Infrastructure Components

- **VPC**: Isolated network with public/private subnets
- **RDS**: PostgreSQL database with encryption and backups
- **S3**: Data storage with lifecycle policies
- **EC2**: Application server with auto-scaling
- **IAM**: Role-based access control
- **CloudWatch**: Monitoring and logging

### Security Features

- SSL/TLS encryption for database connections
- S3 bucket encryption and versioning
- IAM roles with least privilege access
- Security groups with minimal required access
- VPC isolation and NAT gateways

## 📈 Monitoring and Operations

### Health Checks
- Database connectivity monitoring
- S3 access verification
- API endpoint availability
- Risk score calculation status

### Logging
- Structured logging with JSON format
- CloudWatch log aggregation
- Error tracking and alerting
- Performance metrics collection

### Automation
- Daily data pipeline execution
- Automated risk score updates
- Infrastructure monitoring
- Backup and recovery procedures

## 🔧 Configuration

### Environment Variables

```bash
# Database
PGHOST=your-rds-endpoint
PGPORT=5432
PGDATABASE=iapd
PGUSER=iapdadmin
PGPASSWORD=your-password

# AWS
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1

# S3 Buckets
S3_RAW_BUCKET=your-raw-data-bucket
S3_PROCESSED_BUCKET=your-processed-data-bucket
```

### Database Schema

#### ia_filing Table
```sql
CREATE TABLE ia_filing (
  crd             INT,
  filing_date     DATE,
  raum            NUMERIC,
  total_clients   INT,
  total_accounts  INT,
  cco_id          TEXT,
  disclosure_flag CHAR(1),
  PRIMARY KEY (crd, filing_date)
);
```

#### risk_scores Table
```sql
CREATE TABLE risk_scores (
  crd                     INTEGER PRIMARY KEY,
  firm_name               TEXT,
  overall_risk_score      DECIMAL(5,4),
  risk_category           VARCHAR(20),
  disclosure_risk         DECIMAL(5,4),
  aum_volatility_risk     DECIMAL(5,4),
  client_concentration_risk DECIMAL(5,4),
  filing_compliance_risk  DECIMAL(5,4),
  cco_stability_risk      DECIMAL(5,4),
  size_factor_risk        DECIMAL(5,4),
  last_calculation_date   TIMESTAMP,
  risk_factors            JSONB,
  created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🧪 Testing

### Unit Tests
```bash
pytest tests/unit/
```

### Integration Tests
```bash
pytest tests/integration/
```

### API Tests
```bash
pytest tests/api/
```

### Load Testing
```bash
# Test API performance
locust -f tests/load/locustfile.py
```

## 📝 Development Guidelines

### Code Style
- Use Black for Python formatting
- Follow PEP 8 guidelines
- Use type hints throughout
- Write comprehensive docstrings

### Git Workflow
- Feature branches for new development
- Pull requests for code review
- Semantic versioning for releases
- Automated testing on commits

### Security Best Practices
- Never commit secrets or credentials
- Use environment variables for configuration
- Implement proper input validation
- Regular security dependency updates

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

For questions and support:
- Create an issue in the GitHub repository
- Check the documentation in the `/docs` folder
- Review the troubleshooting guide

## 🔄 Roadmap

- [ ] Machine learning-based risk prediction
- [ ] Real-time alerting system
- [ ] Advanced analytics dashboard
- [ ] Multi-region deployment
- [ ] API rate limiting and caching
- [ ] Enhanced data validation
- [ ] Automated compliance reporting
- [ ] Integration with external data sources
