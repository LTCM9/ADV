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
  factors?: {
    aum_drop_pct?: number;
    client_drop_pct?: number;
    acct_drop_pct?: number;
    cco_changed?: boolean;
    new_disc?: boolean;
    trend_down?: boolean;
    small_raum?: boolean;
    adviser_age_yrs?: number;
    owner_moves_12m?: number;
  };
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