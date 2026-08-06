"use client";

import { useTheme } from "next-themes";
import { Search, Sun, Moon, Bell, LogOut, User as UserIcon, Settings } from "lucide-react";
import { useAuthStore } from "@/lib/store";
import { useSyncExternalStore } from "react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const emptySubscribe = () => () => {};

export default function TopHeader() {
  const { theme, setTheme } = useTheme();
  const { user, logout } = useAuthStore();
  const mounted = useSyncExternalStore(
    emptySubscribe,
    () => true,
    () => false
  );

  return (
    <header className="h-16 border-b border-border bg-background flex items-center justify-between px-6 sticky top-0 z-10 ml-64">
      {/* Search Bar */}
      <div className="flex-1 max-w-md">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search markets, stocks, crypto, news..."
            className="w-full bg-muted/50 border border-border rounded-full pl-10 pr-4 py-2 text-sm focus:outline-none focus:ring-1 focus:ring-primary focus:bg-background transition-all"
          />
          <div className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-muted-foreground bg-muted px-1.5 py-0.5 rounded border border-border">
            /
          </div>
        </div>
      </div>

      {/* Tickers */}
      <div className="hidden lg:flex items-center gap-6 mx-8">
        <div className="flex flex-col">
          <span className="text-xs text-muted-foreground font-medium">NIFTY 50</span>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold">22,529.15</span>
            <span className="text-xs font-medium text-primary">+1.35%</span>
          </div>
        </div>
        <div className="h-8 w-px bg-border"></div>
        <div className="flex flex-col">
          <span className="text-xs text-muted-foreground font-medium">BANK NIFTY</span>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold">48,103.25</span>
            <span className="text-xs font-medium text-destructive">-0.48%</span>
          </div>
        </div>
        <div className="h-8 w-px bg-border"></div>
        <div className="flex flex-col">
          <span className="text-xs text-muted-foreground font-medium">BTC/USDT</span>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold">68,957.12</span>
            <span className="text-xs font-medium text-primary">+2.34%</span>
          </div>
        </div>
      </div>

      {/* Right Actions */}
      <div className="flex items-center gap-4">
        {mounted && (
          <button
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
            className="p-2 text-muted-foreground hover:text-foreground rounded-full hover:bg-muted transition-colors"
          >
            {theme === "dark" ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
          </button>
        )}
        <button className="p-2 text-muted-foreground hover:text-foreground rounded-full hover:bg-muted transition-colors relative">
          <Bell className="w-5 h-5" />
          <span className="absolute top-1.5 right-1.5 w-2 h-2 bg-primary rounded-full"></span>
        </button>
        <DropdownMenu>
          <DropdownMenuTrigger className="focus:outline-none">
            <div className="flex items-center gap-2 ml-2 pl-4 border-l border-border">
              <div className="w-8 h-8 rounded-full bg-muted flex items-center justify-center border border-border cursor-pointer hover:border-primary transition-colors">
                <span className="text-sm font-semibold">{user?.email ? user.email.substring(0, 2).toUpperCase() : "VS"}</span>
              </div>
            </div>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <DropdownMenuGroup>
              <DropdownMenuLabel className="font-normal">
                <div className="flex flex-col space-y-1">
                  <p className="text-sm font-medium leading-none">{user?.email || "User"}</p>
                  <p className="text-xs leading-none text-muted-foreground">
                    {user?.id ? "TradeSphere Pro" : "Guest"}
                  </p>
                </div>
              </DropdownMenuLabel>
            </DropdownMenuGroup>
            <DropdownMenuSeparator />
            <DropdownMenuItem className="cursor-pointer">
              <UserIcon className="mr-2 h-4 w-4" />
              <span>Profile</span>
            </DropdownMenuItem>
            <DropdownMenuItem className="cursor-pointer">
              <Settings className="mr-2 h-4 w-4" />
              <span>Settings</span>
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={logout} className="cursor-pointer text-destructive focus:text-destructive">
              <LogOut className="mr-2 h-4 w-4" />
              <span>Log out</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
}
