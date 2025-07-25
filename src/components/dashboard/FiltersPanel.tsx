import { useState } from "react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { Search, Filter, X } from "lucide-react";

interface FiltersProps {
  onFiltersChange: (filters: FilterState) => void;
}

export interface FilterState {
  search: string;
  status: string;
  hasNewDisclosures: boolean;
  complianceScore: string;
  aumRange: string;
  state: string;
}

const STATES = [
  'All States', 'NY', 'CA', 'TX', 'FL', 'IL', 'PA', 'CT', 'MA', 'NJ', 'OH'
];

const AUM_RANGES = [
  'All Ranges',
  'Under $1B',
  '$1B - $10B', 
  '$10B - $100B',
  '$100B - $1T',
  'Over $1T'
];

export const FiltersPanel = ({ onFiltersChange }: FiltersProps) => {
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    status: 'All',
    hasNewDisclosures: false,
    complianceScore: 'All',
    aumRange: 'All Ranges',
    state: 'All States'
  });

  const [isExpanded, setIsExpanded] = useState(false);

  const updateFilter = (key: keyof FilterState, value: string | boolean) => {
    const newFilters = { ...filters, [key]: value };
    setFilters(newFilters);
    onFiltersChange(newFilters);
  };

  const clearFilters = () => {
    const clearedFilters: FilterState = {
      search: '',
      status: 'All',
      hasNewDisclosures: false,
      complianceScore: 'All',
      aumRange: 'All Ranges',
      state: 'All States'
    };
    setFilters(clearedFilters);
    onFiltersChange(clearedFilters);
  };

  return (
    <Card className="p-4 bg-gradient-card border-border">
      <div className="space-y-4">
        {/* Search Bar */}
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            placeholder="Search firms by name, CRD, or SEC number..."
            value={filters.search}
            onChange={(e) => updateFilter('search', e.target.value)}
            className="pl-10 transition-smooth focus:ring-financial-primary"
          />
        </div>

        {/* Toggle Advanced Filters */}
        <div className="flex items-center justify-between">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setIsExpanded(!isExpanded)}
            className="flex items-center gap-2"
          >
            <Filter className="h-4 w-4" />
            Advanced Filters
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={clearFilters}
            className="flex items-center gap-1 text-muted-foreground hover:text-foreground"
          >
            <X className="h-3 w-3" />
            Clear
          </Button>
        </div>

        {/* Advanced Filters */}
        {isExpanded && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-4 border-t border-border">
            {/* Status Filter */}
            <div className="space-y-2">
              <Label className="text-sm font-medium">Status</Label>
              <Select value={filters.status} onValueChange={(value) => updateFilter('status', value)}>
                <SelectTrigger className="transition-smooth focus:ring-financial-primary">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="All">All Status</SelectItem>
                  <SelectItem value="Active">Active</SelectItem>
                  <SelectItem value="Inactive">Inactive</SelectItem>
                  <SelectItem value="Suspended">Suspended</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* AUM Range */}
            <div className="space-y-2">
              <Label className="text-sm font-medium">AUM Range</Label>
              <Select value={filters.aumRange} onValueChange={(value) => updateFilter('aumRange', value)}>
                <SelectTrigger className="transition-smooth focus:ring-financial-primary">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {AUM_RANGES.map((range) => (
                    <SelectItem key={range} value={range}>{range}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* State Filter */}
            <div className="space-y-2">
              <Label className="text-sm font-medium">State</Label>
              <Select value={filters.state} onValueChange={(value) => updateFilter('state', value)}>
                <SelectTrigger className="transition-smooth focus:ring-financial-primary">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {STATES.map((state) => (
                    <SelectItem key={state} value={state}>{state}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {/* Compliance Score */}
            <div className="space-y-2">
              <Label className="text-sm font-medium">Compliance Score</Label>
              <Select value={filters.complianceScore} onValueChange={(value) => updateFilter('complianceScore', value)}>
                <SelectTrigger className="transition-smooth focus:ring-financial-primary">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="All">All Scores</SelectItem>
                  <SelectItem value="90+">90% and above</SelectItem>
                  <SelectItem value="75-89">75% - 89%</SelectItem>
                  <SelectItem value="Below 75">Below 75%</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {/* New Disclosures Checkbox */}
            <div className="space-y-2">
              <Label className="text-sm font-medium">Alerts</Label>
              <div className="flex items-center space-x-2">
                <Checkbox
                  id="newDisclosures"
                  checked={filters.hasNewDisclosures}
                  onCheckedChange={(checked) => updateFilter('hasNewDisclosures', checked as boolean)}
                  className="border-financial-primary data-[state=checked]:bg-financial-primary"
                />
                <Label htmlFor="newDisclosures" className="text-sm">
                  Has new disclosures
                </Label>
              </div>
            </div>
          </div>
        )}
      </div>
    </Card>
  );
};