import { AdvisorFirm, DashboardStats } from '@/types/advisor';

export const mockFirms: AdvisorFirm[] = [
  {
    id: '1',
    name: 'Goldman Sachs Asset Management',
    crd: '79218',
    sec_number: '801-69815',
    address: {
      street: '200 West Street',
      city: 'New York',
      state: 'NY',
      zip: '10282'
    },
    aum: 2400000000000, // $2.4T
    employees: 12000,
    clients: 450,
    last_updated: '2025-01-20',
    compliance_score: 85,
    status: 'Active',
    disclosures: [
      {
        id: 'd1',
        type: 'Fine',
        date: '2025-01-18',
        description: 'Failure to properly supervise municipal bond trading activities',
        amount: 15000000,
        regulator: 'SEC',
        severity: 'High',
        is_new: true
      },
      {
        id: 'd2',
        type: 'Regulatory',
        date: '2024-12-15',
        description: 'Inadequate compliance procedures for ESG investment claims',
        amount: 2500000,
        regulator: 'SEC',
        severity: 'Medium',
        is_new: false
      }
    ]
  },
  {
    id: '2',
    name: 'BlackRock Investment Management',
    crd: '103570',
    sec_number: '801-56594',
    address: {
      street: '55 East 52nd Street',
      city: 'New York',
      state: 'NY',
      zip: '10055'
    },
    aum: 9600000000000, // $9.6T
    employees: 18000,
    clients: 320,
    last_updated: '2025-01-19',
    compliance_score: 92,
    status: 'Active',
    disclosures: [
      {
        id: 'd3',
        type: 'Regulatory',
        date: '2025-01-15',
        description: 'Disclosure deficiencies in proxy voting procedures',
        regulator: 'SEC',
        severity: 'Low',
        is_new: true
      }
    ]
  },
  {
    id: '3',
    name: 'Vanguard Group Inc',
    crd: '104038',
    sec_number: '801-57900',
    address: {
      street: '100 Vanguard Blvd',
      city: 'Malvern',
      state: 'PA',
      zip: '19355'
    },
    aum: 7800000000000, // $7.8T
    employees: 17000,
    clients: 280,
    last_updated: '2025-01-17',
    compliance_score: 96,
    status: 'Active',
    disclosures: []
  },
  {
    id: '4',
    name: 'Bridgewater Associates',
    crd: '103441',
    sec_number: '801-56693',
    address: {
      street: '1 Glendinning Pl',
      city: 'Westport',
      state: 'CT',
      zip: '06880'
    },
    aum: 150000000000, // $150B
    employees: 1500,
    clients: 65,
    last_updated: '2025-01-16',
    compliance_score: 78,
    status: 'Active',
    disclosures: [
      {
        id: 'd4',
        type: 'Disciplinary',
        date: '2025-01-20',
        description: 'Failure to maintain adequate books and records for client communications',
        amount: 850000,
        regulator: 'CFTC',
        severity: 'Medium',
        is_new: true
      },
      {
        id: 'd5',
        type: 'Fine',
        date: '2024-11-30',
        description: 'Improper handling of material non-public information',
        amount: 3200000,
        regulator: 'SEC',
        severity: 'High',
        is_new: false
      }
    ]
  },
  {
    id: '5',
    name: 'Citadel Advisors LLC',
    crd: '148826',
    sec_number: '801-74866',
    address: {
      street: '131 S Dearborn St',
      city: 'Chicago',
      state: 'IL',
      zip: '60603'
    },
    aum: 54000000000, // $54B
    employees: 2800,
    clients: 89,
    last_updated: '2025-01-21',
    compliance_score: 81,
    status: 'Active',
    disclosures: [
      {
        id: 'd6',
        type: 'Fine',
        date: '2025-01-21',
        description: 'Violations of best execution requirements in equity trading',
        amount: 22000000,
        regulator: 'FINRA',
        severity: 'Critical',
        is_new: true
      }
    ]
  }
];

export const mockStats: DashboardStats = {
  total_firms: 14750,
  new_disclosures: 47,
  high_severity_alerts: 12,
  total_aum: 42500000000000, // $42.5T
  firms_with_recent_activity: 156
};