import { Card } from "@/components/ui/card";
import { LucideIcon } from "lucide-react";

interface StatsCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: LucideIcon;
  trend?: {
    value: number;
    isPositive: boolean;
  };
  variant?: 'default' | 'warning' | 'success' | 'danger';
}

export const StatsCard = ({ 
  title, 
  value, 
  subtitle, 
  icon: Icon, 
  trend,
  variant = 'default' 
}: StatsCardProps) => {
  const getVariantStyles = () => {
    switch (variant) {
      case 'warning':
        return 'border-financial-warning/30 bg-gradient-alert';
      case 'success':
        return 'border-financial-success/30 bg-gradient-primary';
      case 'danger':
        return 'border-financial-danger/30 bg-gradient-alert';
      default:
        return 'border-border bg-gradient-card';
    }
  };

  const getIconColor = () => {
    switch (variant) {
      case 'warning':
        return 'text-financial-warning';
      case 'success':
        return 'text-financial-success';
      case 'danger':
        return 'text-financial-danger';
      default:
        return 'text-financial-primary';
    }
  };

  return (
    <Card className={`p-6 transition-smooth hover:shadow-elegant ${getVariantStyles()}`}>
      <div className="flex items-center justify-between">
        <div className="flex-1">
          <p className="text-sm font-medium text-muted-foreground">{title}</p>
          <div className="flex items-baseline gap-2 mt-2">
            <p className="text-2xl font-bold text-foreground">{value}</p>
            {trend && (
              <span 
                className={`text-xs font-medium ${
                  trend.isPositive ? 'text-financial-success' : 'text-financial-danger'
                }`}
              >
                {trend.isPositive ? '+' : ''}{trend.value}%
              </span>
            )}
          </div>
          {subtitle && (
            <p className="text-xs text-muted-foreground mt-1">{subtitle}</p>
          )}
        </div>
        <div className={`p-3 rounded-lg bg-background/50 ${getIconColor()}`}>
          <Icon className="h-6 w-6" />
        </div>
      </div>
    </Card>
  );
};