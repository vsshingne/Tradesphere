"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  LayoutDashboard,
  LineChart,
  Wallet,
  Briefcase,
  ListOrdered,
  PieChart,
  Newspaper,
  Bell,
  Settings,
  Target,
  ArrowRight
} from "lucide-react";

const navItems = [
  { name: "Dashboard", href: "/dashboard", icon: LayoutDashboard },
  { name: "Watchlist", href: "/watchlist", icon: Target },
  { name: "Markets", href: "/markets", icon: LineChart },
  { name: "Portfolio", href: "/portfolio", icon: Briefcase },
  { name: "Orders", href: "/orders", icon: ListOrdered },
  { name: "Positions", href: "/positions", icon: PieChart },
  { name: "Funds", href: "/funds", icon: Wallet },
  { name: "Analytics", href: "/analytics", icon: LineChart },
  { name: "Screener", href: "/screener", icon: Target },
  { name: "News", href: "/news", icon: Newspaper },
  { name: "Alerts", href: "/alerts", icon: Bell },
  { name: "Settings", href: "/settings", icon: Settings },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-64 border-r border-border bg-background flex flex-col h-screen fixed top-0 left-0 overflow-y-auto">
      {/* Logo Area */}
      <div className="flex h-16 items-center px-6">
        <div className="flex items-center gap-2 text-primary">
          <div className="flex items-center justify-center w-8 h-8 rounded-full border-2 border-primary">
            <span className="font-bold text-lg leading-none mt-[-2px]">T</span>
          </div>
          <span className="font-bold text-xl tracking-wide text-foreground">TRADESPHERE</span>
        </div>
      </div>

      {/* Navigation Links */}
      <nav className="flex-1 px-4 py-4 space-y-1">
        {navItems.map((item) => {
          const isActive = pathname === item.href || (pathname === "/trade" && item.href === "/dashboard");
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex items-center gap-3 px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                isActive
                  ? "bg-primary/10 text-primary"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground"
              }`}
            >
              <item.icon className={`w-5 h-5 ${isActive ? "text-primary" : ""}`} />
              {item.name}
            </Link>
          );
        })}
      </nav>

      {/* Upgrade Card */}
      <div className="p-4 mt-auto">
        <div className="rounded-lg border border-border bg-card p-4 space-y-3">
          <div className="flex items-center gap-2">
            <Target className="w-4 h-4 text-primary" />
            <span className="text-sm font-semibold text-foreground">TradeSphere Pro</span>
          </div>
          <p className="text-xs text-muted-foreground">Unlock advanced tools, real-time insights & more.</p>
          <button className="w-full bg-primary text-primary-foreground hover:bg-primary/90 rounded-md py-2 px-4 text-xs font-semibold flex items-center justify-between transition-colors">
            Upgrade Now <ArrowRight className="w-3 h-3" />
          </button>
        </div>
      </div>
    </aside>
  );
}
