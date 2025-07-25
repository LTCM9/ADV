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

  const getComplianceColor = (score: number) => {
    if (score >= 90) return 'text-financial-success';
    if (score >= 75) return 'text-financial-warning';
    return 'text-financial-danger';
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'Active': return 'bg-financial-success/10 text-financial-success border-financial-success/30';
      case 'Suspended': return 'bg-financial-danger/10 text-financial-danger border-financial-danger/30';
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
              <span>CRD: {firm.crd}</span>
              <span>SEC: {firm.sec_number}</span>
            </div>
          </div>
          <div className="flex flex-col items-end gap-2">
            <Badge className={getStatusColor(firm.status)}>
              {firm.status}
            </Badge>
            {(newDisclosures.length > 0 || hasHighSeverityDisclosures) && (
              <div className="flex items-center gap-1 text-financial-warning">
                <AlertTriangle className="h-4 w-4" />
                <span className="text-xs font-medium">
                  {newDisclosures.length > 0 ? `${newDisclosures.length} New` : 'High Risk'}
                </span>
              </div>
            )}
          </div>
        </div>

        {/* Key Metrics */}
        <div className="grid grid-cols-2 gap-4">
          <div className="flex items-center gap-2">
            <DollarSign className="h-4 w-4 text-financial-accent" />
            <div>
              <p className="text-sm font-medium text-foreground">{formatAUM(firm.aum)}</p>
              <p className="text-xs text-muted-foreground">AUM</p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Users className="h-4 w-4 text-financial-secondary" />
            <div>
              <p className="text-sm font-medium text-foreground">{firm.clients}</p>
              <p className="text-xs text-muted-foreground">Clients</p>
            </div>
          </div>
        </div>

        {/* Location & Compliance */}
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <MapPin className="h-4 w-4" />
            <span>{firm.address.city}, {firm.address.state}</span>
          </div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-sm">
              <span className="text-muted-foreground">Compliance Score:</span>
              <span className={`font-medium ${getComplianceColor(firm.compliance_score)}`}>
                {firm.compliance_score}%
              </span>
            </div>
            <div className="flex items-center gap-1 text-xs text-muted-foreground">
              <Calendar className="h-3 w-3" />
              <span>Updated {firm.last_updated}</span>
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