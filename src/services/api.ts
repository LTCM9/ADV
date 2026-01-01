const API_BASE_URL = 'http://localhost:3001/api';

export interface RiskStatistics {
  risk_category: string;
  firm_count: string;
  percentage: string;
  avg_score: string;
}

export interface Firm {
  sec_number: string;
  firm_name: string;
  score: number;
  risk_category: string;
  filing_date: string;
  factors: {
    new_disc: number;
    small_raum: number;
    trend_down: number;
    cco_changed: number;
    aum_drop_pct: number;
    acct_drop_pct: number;
    adviser_age_yrs: number;
    client_drop_pct: number;
    owner_moves_12m: number;
  };
}

export interface DashboardSummary {
  riskStatistics: RiskStatistics[];
  totalFirms: number;
  recentFilings: number;
}

class ApiService {
  private async fetchWithErrorHandling<T>(endpoint: string): Promise<T> {
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      return await response.json();
    } catch (error) {
      console.error(`API Error (${endpoint}):`, error);
      throw error;
    }
  }

  async getRiskStatistics(): Promise<RiskStatistics[]> {
    return this.fetchWithErrorHandling<RiskStatistics[]>('/risk-statistics');
  }

  async getFirms(riskCategory?: string, limit: number = 100): Promise<Firm[]> {
    const endpoint = riskCategory ? `/firms/${riskCategory}?limit=${limit}` : `/firms?limit=${limit}`;
    return this.fetchWithErrorHandling<Firm[]>(endpoint);
  }

  async getTopRiskyFirms(limit: number = 10): Promise<Firm[]> {
    return this.fetchWithErrorHandling<Firm[]>(`/top-risky-firms?limit=${limit}`);
  }

  async getDashboardSummary(): Promise<DashboardSummary> {
    return this.fetchWithErrorHandling<DashboardSummary>('/dashboard-summary');
  }

  async healthCheck(): Promise<{ status: string; timestamp: string }> {
    return this.fetchWithErrorHandling<{ status: string; timestamp: string }>('/health');
  }
}

export const apiService = new ApiService(); 