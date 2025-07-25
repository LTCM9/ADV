import { useState, useMemo } from "react";
import { AdvisorFirm } from "@/types/advisor";
import { mockFirms, mockStats } from "@/data/mockData";
import { StatsCard } from "./StatsCard";
import { FirmCard } from "./FirmCard";
import { FiltersPanel, FilterState } from "./FiltersPanel";
import { Building2, AlertTriangle, DollarSign, TrendingUp, Bell } from "lucide-react";
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

  // Filter firms based on current filters
  const filteredFirms = useMemo(() => {
    return mockFirms.filter(firm => {
      // Search filter
      if (filters.search) {
        const searchTerm = filters.search.toLowerCase();
        if (!firm.name.toLowerCase().includes(searchTerm) &&
            !firm.crd.includes(searchTerm) &&
            !firm.sec_number.toLowerCase().includes(searchTerm)) {
          return false;
        }
      }

      // Status filter
      if (filters.status !== 'All' && firm.status !== filters.status) {
        return false;
      }

      // New disclosures filter
      if (filters.hasNewDisclosures && !firm.disclosures.some(d => d.is_new)) {
        return false;
      }

      // Compliance score filter
      if (filters.complianceScore !== 'All') {
        if (filters.complianceScore === '90+' && firm.compliance_score < 90) return false;
        if (filters.complianceScore === '75-89' && (firm.compliance_score < 75 || firm.compliance_score >= 90)) return false;
        if (filters.complianceScore === 'Below 75' && firm.compliance_score >= 75) return false;
      }

      // State filter
      if (filters.state !== 'All States' && firm.address.state !== filters.state) {
        return false;
      }

      // AUM Range filter
      if (filters.aumRange !== 'All Ranges') {
        const aum = firm.aum;
        switch (filters.aumRange) {
          case 'Under $1B':
            if (aum >= 1e9) return false;
            break;
          case '$1B - $10B':
            if (aum < 1e9 || aum >= 10e9) return false;
            break;
          case '$10B - $100B':
            if (aum < 10e9 || aum >= 100e9) return false;
            break;
          case '$100B - $1T':
            if (aum < 100e9 || aum >= 1e12) return false;
            break;
          case 'Over $1T':
            if (aum < 1e12) return false;
            break;
        }
      }

      return true;
    });
  }, [filters]);

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
                {mockStats.new_disclosures} New Alerts
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
            value={mockStats.total_firms.toLocaleString()}
            icon={Building2}
            subtitle="Registered advisors"
          />
          <StatsCard
            title="New Disclosures"
            value={mockStats.new_disclosures}
            icon={AlertTriangle}
            variant="warning"
            subtitle="Last 30 days"
            trend={{ value: 12, isPositive: false }}
          />
          <StatsCard
            title="High Risk Alerts"
            value={mockStats.high_severity_alerts}
            icon={AlertTriangle}
            variant="danger"
            subtitle="Critical & High severity"
          />
          <StatsCard
            title="Total AUM"
            value={formatAUM(mockStats.total_aum)}
            icon={DollarSign}
            subtitle="Assets under management"
          />
          <StatsCard
            title="Active Monitoring"
            value={mockStats.firms_with_recent_activity}
            icon={TrendingUp}
            variant="success"
            subtitle="Firms with recent activity"
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
              Showing {filteredFirms.length} of {mockFirms.length} firms
            </p>
          </div>
        </div>

        {/* Firms Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
          {filteredFirms.map((firm) => (
            <FirmCard
              key={firm.id}
              firm={firm}
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