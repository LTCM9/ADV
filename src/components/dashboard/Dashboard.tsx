import { useState, useMemo, useEffect } from "react";
import { AdvisorFirm } from "@/types/advisor";
import { apiService, type DashboardSummary, type Firm } from "@/services/api";
import { StatsCard } from "./StatsCard";
import { FirmCard } from "./FirmCard";
import { FiltersPanel, FilterState } from "./FiltersPanel";
import { Building2, AlertTriangle, DollarSign, TrendingUp, Bell, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

export const Dashboard = () => {
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    status: 'All',
    hasNewDisclosures: false,
    complianceScore: 'All',
    aumRange: 'All Ranges',
    state: 'All States'
  });

  const [selectedFirm, setSelectedFirm] = useState<AdvisorFirm | null>(null);
  
  // Real data state
  const [dashboardData, setDashboardData] = useState<DashboardSummary | null>(null);
  const [firms, setFirms] = useState<Firm[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch real data
  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        setError(null);
        
        // Fetch dashboard summary and firms in parallel
        const [summaryData, firmsData] = await Promise.all([
          apiService.getDashboardSummary(),
          apiService.getFirms(undefined, 50) // Get first 50 firms
        ]);
        
        setDashboardData(summaryData);
        setFirms(firmsData);
      } catch (err) {
        console.error('Failed to fetch data:', err);
        setError('Failed to load data. Please check if the API server is running.');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // Filter firms based on current filters
  const filteredFirms = useMemo(() => {
    if (!firms.length) return [];
    
    return firms.filter(firm => {
      // Search filter
      if (filters.search) {
        const searchTerm = filters.search.toLowerCase();
        if (!firm.firm_name.toLowerCase().includes(searchTerm) &&
            !firm.sec_number.toLowerCase().includes(searchTerm)) {
          return false;
        }
      }

      // Risk category filter
      if (filters.status !== 'All' && firm.risk_category !== filters.status) {
        return false;
      }

      // New disclosures filter
      if (filters.hasNewDisclosures && !firm.factors.new_disc) {
        return false;
      }

      // Risk score filter (using actual risk score ranges)
      if (filters.complianceScore !== 'All') {
        if (filters.complianceScore === '80+ (Critical)' && firm.score < 80) return false;
        if (filters.complianceScore === '60-79 (High)' && (firm.score < 60 || firm.score >= 80)) return false;
        if (filters.complianceScore === 'Below 60 (Medium/Low)' && firm.score >= 60) return false;
      }

      return true;
    });
  }, [firms, filters]);

  const formatAUM = (amount: number) => {
    if (amount >= 1e12) return `$${(amount / 1e12).toFixed(1)}T`;
    if (amount >= 1e9) return `$${(amount / 1e9).toFixed(1)}B`;
    return `$${amount.toLocaleString()}`;
  };

  const handleViewDetails = (firm: AdvisorFirm) => {
    setSelectedFirm(firm);
    // In a real app, this would navigate to a detail page
    console.log('Viewing details for:', firm.name);
  };

  // Show loading state
  if (loading) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4 text-primary" />
          <h2 className="text-lg font-medium">Loading dashboard data...</h2>
          <p className="text-sm text-muted-foreground">Please wait while we fetch the latest risk data</p>
        </div>
      </div>
    );
  }

  // Show error state
  if (error) {
    return (
      <div className="min-h-screen bg-background flex items-center justify-center">
        <div className="text-center">
          <AlertTriangle className="h-8 w-8 mx-auto mb-4 text-destructive" />
          <h2 className="text-lg font-medium text-destructive">Failed to load data</h2>
          <p className="text-sm text-muted-foreground mb-4">{error}</p>
          <Button onClick={() => window.location.reload()}>Retry</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="border-b border-border bg-gradient-card">
        <div className="container mx-auto px-6 py-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-foreground">Investment Advisor Dashboard</h1>
              <p className="text-muted-foreground mt-1">Monitor regulatory disclosures and firm compliance</p>
            </div>
            <div className="flex items-center gap-4">
              <Badge className="bg-financial-warning/10 text-financial-warning border-financial-warning/30">
                <Bell className="h-3 w-3 mr-1" />
                {dashboardData?.recentFilings || 0} New Alerts
              </Badge>
              <Button className="bg-financial-primary hover:bg-financial-primary/90 text-white">
                Export Data
              </Button>
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-6 py-6">
        {/* Stats Overview */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-6 mb-8">
          <StatsCard
            title="Total Firms"
            value={dashboardData?.totalFirms.toLocaleString() || '0'}
            icon={Building2}
            subtitle="Registered advisors"
          />
          <StatsCard
            title="Critical Risk"
            value={dashboardData?.riskStatistics.find(s => s.risk_category === 'Critical')?.firm_count || '0'}
            icon={AlertTriangle}
            variant="danger"
            subtitle="Critical risk firms"
          />
          <StatsCard
            title="High Risk"
            value={dashboardData?.riskStatistics.find(s => s.risk_category === 'High')?.firm_count || '0'}
            icon={AlertTriangle}
            variant="warning"
            subtitle="High risk firms"
          />
          <StatsCard
            title="Medium Risk"
            value={dashboardData?.riskStatistics.find(s => s.risk_category === 'Medium')?.firm_count || '0'}
            icon={TrendingUp}
            variant="default"
            subtitle="Medium risk firms"
          />
          <StatsCard
            title="Low Risk"
            value={dashboardData?.riskStatistics.find(s => s.risk_category === 'Low')?.firm_count || '0'}
            icon={TrendingUp}
            variant="success"
            subtitle="Low risk firms"
          />
        </div>

        {/* Filters */}
        <div className="mb-6">
          <FiltersPanel onFiltersChange={setFilters} />
        </div>

        {/* Results Header */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-xl font-semibold text-foreground">
              Advisor Firms ({filteredFirms.length})
            </h2>
            <p className="text-sm text-muted-foreground">
              Showing {filteredFirms.length} of {firms.length} firms
            </p>
          </div>
        </div>

        {/* Firms Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
          {filteredFirms.map((firm) => (
            <FirmCard
              key={firm.sec_number}
              firm={{
                id: firm.sec_number,
                name: firm.firm_name,
                crd: firm.sec_number,
                sec_number: firm.sec_number,
                status: firm.risk_category,
                compliance_score: firm.score,
                aum: firm.raum || 0,
                clients: firm.client_count || 0,
                address: { city: 'N/A', state: 'N/A' }, // Not available in current data
                disclosures: firm.factors?.new_disc ? [{ is_new: true }] : [],
                last_updated: firm.latest_filing_date || firm.filing_date,
                factors: firm.factors // Pass the risk factors data
              }}
              onViewDetails={handleViewDetails}
            />
          ))}
        </div>

        {filteredFirms.length === 0 && (
          <div className="text-center py-12">
            <Building2 className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
            <h3 className="text-lg font-medium text-foreground mb-2">No firms found</h3>
            <p className="text-muted-foreground">Try adjusting your filters to see more results.</p>
          </div>
        )}
      </div>
    </div>
  );
};