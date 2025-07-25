export interface AdvisorFirm {
  id: string;
  name: string;
  crd: string;
  sec_number: string;
  address: {
    street: string;
    city: string;
    state: string;
    zip: string;
  };
  aum: number; // Assets Under Management
  employees: number;
  clients: number;
  last_updated: string;
  disclosures: Disclosure[];
  compliance_score: number;
  status: 'Active' | 'Inactive' | 'Suspended';
}

export interface Disclosure {
  id: string;
  type: 'Fine' | 'Disciplinary' | 'Regulatory' | 'Criminal' | 'Civil';
  date: string;
  description: string;
  amount?: number;
  regulator: string;
  severity: 'Low' | 'Medium' | 'High' | 'Critical';
  is_new: boolean;
}

export interface DashboardStats {
  total_firms: number;
  new_disclosures: number;
  high_severity_alerts: number;
  total_aum: number;
  firms_with_recent_activity: number;
}