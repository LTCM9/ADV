import { AdvisorFirm } from "@/types/advisor";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { AlertTriangle, Building2, DollarSign, Users, MapPin, Calendar } from "lucide-react";

interface FirmCardProps {
  firm: AdvisorFirm;
  onViewDetails: (firm: AdvisorFirm) => void;
}

export const FirmCard = ({ firm, onViewDetails }: FirmCardProps) => {
  const formatAUM = (amount: number) => {
    if (amount >= 1e12) return `$${(amount / 1e12).toFixed(1)}T`;
    if (amount >= 1e9) return `$${(amount / 1e9).toFixed(1)}B`;
    if (amount >= 1e6) return `$${(amount / 1e6).toFixed(1)}M`;
    return `$${amount.toLocaleString()}`;
  };

  const getRiskScoreColor = (score: number) => {
    if (score >= 80) return 'text-financial-danger'; // Critical
    if (score >= 60) return 'text-financial-warning'; // High
    if (score >= 30) return 'text-financial-accent'; // Medium
    return 'text-financial-success'; // Low
  };

  const getRiskCategoryColor = (category: string) => {
    switch (category) {
      case 'Critical': return 'bg-financial-danger/10 text-financial-danger border-financial-danger/30';
      case 'High': return 'bg-financial-warning/10 text-financial-warning border-financial-warning/30';
      case 'Medium': return 'bg-financial-accent/10 text-financial-accent border-financial-accent/30';
      case 'Low': return 'bg-financial-success/10 text-financial-success border-financial-success/30';
      default: return 'bg-muted text-muted-foreground';
    }
  };

  const newDisclosures = firm.disclosures.filter(d => d.is_new);
  const hasHighSeverityDisclosures = firm.disclosures.some(d => d.severity === 'High' || d.severity === 'Critical');

  return (
    <Card className="p-6 transition-smooth hover:shadow-elegant bg-gradient-card border-border">
      <div className="space-y-4">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-2">
              <Building2 className="h-5 w-5 text-financial-primary" />
              <h3 className="font-semibold text-lg text-foreground">{firm.name}</h3>
            </div>
            <div className="flex items-center gap-4 text-sm text-muted-foreground">
              <span>SEC: {firm.sec_number}</span>
            </div>
          </div>
          <div className="flex flex-col items-end gap-2">
            <Badge className={getRiskCategoryColor(firm.status)}>
              {firm.status}
            </Badge>
            {firm.disclosures.length > 0 && (
              <div className="flex items-center gap-1 text-financial-warning">
                <AlertTriangle className="h-4 w-4" />
                <span className="text-xs font-medium">
                  New Disclosures
                </span>
              </div>
            )}
          </div>
        </div>

        {/* Key Metrics */}
        <div className="grid grid-cols-3 gap-4">
          <div className="flex items-center gap-2">
            <DollarSign className="h-4 w-4 text-financial-accent" />
            <div>
              <p className="text-sm font-medium text-foreground">
                {firm.aum >= 1000000 ? `$${(firm.aum / 1000000).toFixed(1)}M` : `$${(firm.aum / 1000).toFixed(0)}K`}
              </p>
              <p className="text-xs text-muted-foreground">AUM</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Users className="h-4 w-4 text-financial-secondary" />
            <div>
              <p className="text-sm font-medium text-foreground">
                {firm.clients.toLocaleString()}
              </p>
              <p className="text-xs text-muted-foreground">Clients</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-financial-warning" />
            <div>
              <p className="text-sm font-medium text-foreground">
                {firm.compliance_score}
              </p>
              <p className="text-xs text-muted-foreground">Risk Score</p>
            </div>
          </div>
        </div>

        {/* Risk Details */}
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-sm">
              <span className="text-muted-foreground">Risk Level:</span>
              <span className={`font-medium ${getRiskScoreColor(firm.compliance_score)}`}>
                {firm.compliance_score >= 80 ? 'Critical' : 
                 firm.compliance_score >= 60 ? 'High' : 
                 firm.compliance_score >= 30 ? 'Medium' : 'Low'}
              </span>
            </div>
            <div className="flex items-center gap-1 text-xs text-muted-foreground">
              <Calendar className="h-3 w-3" />
              <span>SEC #{firm.sec_number}</span>
            </div>
          </div>
        </div>

        {/* Risk Scoring Attribution */}
        <div className="space-y-2">
          <p className="text-sm font-medium text-foreground">Risk Factors</p>
          <div className="grid grid-cols-3 gap-2 text-xs">
            <div className={`p-2 rounded-md border ${firm.factors?.aum_drop_pct > 0 ? 'bg-financial-warning/10 border-financial-warning/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">AUM Drop:</span>
                <span className="font-medium">{firm.factors?.aum_drop_pct || 0}%</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.client_drop_pct > 0 ? 'bg-financial-warning/10 border-financial-warning/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Client Drop:</span>
                <span className="font-medium">{firm.factors?.client_drop_pct || 0}%</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.acct_drop_pct > 0 ? 'bg-financial-warning/10 border-financial-warning/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Account Drop:</span>
                <span className="font-medium">{firm.factors?.acct_drop_pct || 0}%</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.cco_changed ? 'bg-financial-warning/10 border-financial-warning/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">CCO Changed:</span>
                <span className="font-medium">{firm.factors?.cco_changed ? 'Yes' : 'No'}</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.new_disc ? 'bg-financial-danger/10 border-financial-danger/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">New Disclosures:</span>
                <span className="font-medium">{firm.factors?.new_disc ? 'Yes' : 'No'}</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.trend_down ? 'bg-financial-warning/10 border-financial-warning/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Trend Down:</span>
                <span className="font-medium">{firm.factors?.trend_down ? 'Yes' : 'No'}</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.small_raum ? 'bg-financial-warning/10 border-financial-warning/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Small AUM:</span>
                <span className="font-medium">{firm.factors?.small_raum ? 'Yes' : 'No'}</span>
              </div>
            </div>
            <div className={`p-2 rounded-md border ${firm.factors?.adviser_age_yrs > 0 ? 'bg-financial-accent/10 border-financial-accent/30' : 'bg-muted/30 border-border'}`}>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Adviser Age:</span>
                <span className="font-medium">{firm.factors?.adviser_age_yrs || 0} yrs</span>
              </div>
            </div>
          </div>
        </div>

        {/* Recent Disclosures */}
        {firm.disclosures.length > 0 && (
          <div className="space-y-2">
            <p className="text-sm font-medium text-foreground">Recent Disclosures</p>
            <div className="space-y-1">
              {firm.disclosures.slice(0, 2).map((disclosure) => (
                <div 
                  key={disclosure.id}
                  className={`p-2 rounded-md border text-xs ${
                    disclosure.is_new 
                      ? 'bg-financial-warning/5 border-financial-warning/30 text-financial-warning' 
                      : 'bg-muted/50 border-border text-muted-foreground'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium">{disclosure.type}</span>
                    <span>{disclosure.date}</span>
                  </div>
                  <p className="truncate mt-1">{disclosure.description}</p>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Action Button */}
        <Button 
          onClick={() => onViewDetails(firm)}
          className="w-full bg-financial-primary hover:bg-financial-primary/90 text-white"
        >
          View Details
        </Button>
      </div>
    </Card>
  );
};