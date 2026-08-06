"use client";

import { useAuthStore } from "@/lib/store";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { AxiosError } from "axios";
import api from "@/lib/api";
import { formatMoney, formatQuantity, cn } from "@/lib/utils";
import { useWebSocket, Side } from "@/lib/useWebSocket";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { toast } from "sonner";
import { Loader2, Clock, Activity, Wallet } from "lucide-react";
import { useState } from "react";
import { AreaChart, Area, YAxis, Tooltip, ResponsiveContainer } from "recharts";

interface Position {
  symbol: string;
  quantity: string | number;
  reserved_quantity?: string | number;
}

interface Portfolio {
  balance?: string | number;
  positions?: Position[];
}

type OrderType = "MARKET" | "LIMIT" | "SL-LIMIT" | "BRACKET";

// Mock Chart Data
const chartData = Array.from({ length: 50 }).map((_, i) => {
  const base = 22400;
  return {
    time: `10:${i.toString().padStart(2, '0')}`,
    value: base + Math.random() * 200 - 100 + (i * 2),
  };
});

const watchlistItems = [
  { symbol: "NIFTY 50", last: "22,529.15", chg: "+300.75", chgPct: "+1.35%", up: true },
  { symbol: "BANK NIFTY", last: "48,103.25", chg: "-232.40", chgPct: "-0.48%", up: false },
  { symbol: "RELIANCE", last: "2,978.65", chg: "+45.30", chgPct: "+1.54%", up: true },
  { symbol: "TCS", last: "3,695.20", chg: "-12.80", chgPct: "-0.35%", up: false },
  { symbol: "HDFCBANK", last: "1,654.75", chg: "+18.45", chgPct: "+1.13%", up: true },
  { symbol: "INFY", last: "1,491.25", chg: "+8.70", chgPct: "+0.59%", up: true },
  { symbol: "BTC/USDT", last: "68,957.12", chg: "+1,581.32", chgPct: "+2.34%", up: true },
  { symbol: "ETH/USDT", last: "3,712.45", chg: "+68.23", chgPct: "+1.87%", up: true },
];

export default function DashboardPage() {
  const { user } = useAuthStore();
  const queryClient = useQueryClient();
  const { trades, openOrders } = useWebSocket();

  const [side, setSide] = useState<Side>("BUY");
  const [type, setType] = useState<OrderType>("LIMIT");
  const [price, setPrice] = useState("50000");
  const [quantity, setQuantity] = useState("1");
  const [selectedSymbol, setSelectedSymbol] = useState("BTC");

  const { data: portfolio } = useQuery<Portfolio | null>({
    queryKey: ["portfolio", user?.id],
    queryFn: async () => {
      if (!user?.id) return null;
      const res = await api.get<Portfolio>(`/portfolio/${user.id}`);
      return res.data;
    },
    enabled: !!user?.id,
  });

  const orderMutation = useMutation({
    mutationFn: async () => {
      const res = await api.post("/orders", {
        user_id: user?.id,
        symbol: selectedSymbol,
        side,
        type,
        price,
        quantity,
      });
      return res.data;
    },
    onSuccess: () => {
      toast.success(`${side} order placed successfully!`);
      queryClient.invalidateQueries({ queryKey: ["portfolio", user?.id] });
    },
    onError: (error: AxiosError<{ error?: string }>) => {
      toast.error(error.response?.data?.error || "Failed to place order");
    },
  });

  const cancelMutation = useMutation({
    mutationFn: async (orderId: string) => {
      const res = await api.post(`/orders/${orderId}/cancel`);
      return res.data;
    },
    onSuccess: () => {
      toast.success("Order canceled");
    },
    onError: (error: AxiosError<{ error?: string }>) => {
      toast.error(error.response?.data?.error || "Failed to cancel order");
    },
  });

  const onSubmitOrder = (e: React.FormEvent) => {
    e.preventDefault();
    if (!price || !quantity) return;
    orderMutation.mutate();
  };

  const orderTypes: OrderType[] = ["MARKET", "LIMIT", "SL-LIMIT", "BRACKET"];

  return (
    <div className="flex flex-col gap-4 h-full">
      {/* Top Welcome & Summary Section */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Welcome back, {user?.email ? user.email.split('@')[0] : "VS Prime"}! 👋</h1>
          <p className="text-sm text-muted-foreground mt-1">Here&apos;s what&apos;s happening in the markets today.</p>
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2 bg-card border border-border rounded-lg px-4 py-2">
            <Clock className="w-4 h-4 text-primary" />
            <span className="text-xs font-semibold">Market Open</span>
          </div>
          <div className="flex items-center gap-2 bg-card border border-border rounded-lg px-4 py-2">
            <Activity className="w-4 h-4 text-blue-500" />
            <span className="text-xs font-semibold">Volatility Normal</span>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-12 gap-4 flex-1 min-h-0">
        
        {/* CENTER COLUMN: Chart & Portfolio */}
        <div className="col-span-12 xl:col-span-8 flex flex-col gap-4">
          {/* Chart Card */}
          <Card className="flex flex-col flex-1 min-h-[400px]">
            <CardHeader className="py-3 px-4 border-b flex flex-row items-center justify-between">
              <div className="flex items-center gap-4">
                <span className="font-bold text-lg">{selectedSymbol}/USDT</span>
                <span className="text-xs text-muted-foreground">1h</span>
                <span className="text-primary text-sm font-semibold">+0.22%</span>
              </div>
              <div className="flex gap-2">
                <Button variant="ghost" size="sm" className="h-8">1H</Button>
                <Button variant="ghost" size="sm" className="h-8">4H</Button>
                <Button variant="ghost" size="sm" className="h-8">1D</Button>
                <Button variant="ghost" size="sm" className="h-8">1W</Button>
              </div>
            </CardHeader>
            <CardContent className="flex-1 p-0 relative">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={chartData} margin={{ top: 20, right: 0, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id="colorValue" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor="#00e5ff" stopOpacity={0.3}/>
                      <stop offset="95%" stopColor="#00e5ff" stopOpacity={0}/>
                    </linearGradient>
                  </defs>
                  <Tooltip 
                    contentStyle={{ backgroundColor: '#131b24', borderColor: '#24303d', borderRadius: '8px' }}
                    itemStyle={{ color: '#00e5ff' }}
                  />
                  <YAxis domain={['auto', 'auto']} hide />
                  <Area type="monotone" dataKey="value" stroke="#00e5ff" strokeWidth={2} fillOpacity={1} fill="url(#colorValue)" />
                </AreaChart>
              </ResponsiveContainer>
              <div className="absolute top-4 left-4 flex gap-4 text-xs text-muted-foreground">
                <p>O: 22,480.15</p>
                <p>H: 22,546.35</p>
                <p>L: 22,430.25</p>
                <p>C: 22,529.15</p>
              </div>
            </CardContent>
          </Card>

          {/* Portfolio Metric Cards */}
          <div className="grid grid-cols-4 gap-4">
            <Card className="bg-card">
              <CardContent className="p-4 space-y-1">
                <p className="text-xs text-muted-foreground">Portfolio Value</p>
                <p className="text-xl font-bold">{formatMoney(portfolio?.balance)}</p>
                <p className="text-xs text-primary">+6.81%</p>
              </CardContent>
            </Card>
            <Card className="bg-card">
              <CardContent className="p-4 space-y-1">
                <p className="text-xs text-muted-foreground">Today&apos;s P&amp;L</p>
                <p className="text-xl font-bold">₹12,540.75</p>
                <p className="text-xs text-primary">+1.89%</p>
              </CardContent>
            </Card>
            <Card className="bg-card">
              <CardContent className="p-4 space-y-1">
                <p className="text-xs text-muted-foreground">Total Invested</p>
                <p className="text-xl font-bold">₹7,20,000.00</p>
              </CardContent>
            </Card>
            <Card className="bg-card">
              <CardContent className="p-4 space-y-1">
                <div className="flex justify-between items-start">
                  <div>
                    <p className="text-xs text-muted-foreground">Available Margin</p>
                    <p className="text-xl font-bold">{formatMoney(portfolio?.balance)}</p>
                  </div>
                  <Wallet className="w-5 h-5 text-primary" />
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Holdings & Orders Tabs */}
          <Card className="flex-1 flex flex-col min-h-[300px]">
            <Tabs defaultValue="holdings" className="flex flex-col h-full">
              <div className="border-b px-4">
                <TabsList className="bg-transparent h-12">
                  <TabsTrigger value="holdings" className="data-[state=active]:bg-transparent data-[state=active]:border-b-2 data-[state=active]:border-primary rounded-none">Holdings</TabsTrigger>
                  <TabsTrigger value="orders" className="data-[state=active]:bg-transparent data-[state=active]:border-b-2 data-[state=active]:border-primary rounded-none">Open Orders</TabsTrigger>
                  <TabsTrigger value="trades" className="data-[state=active]:bg-transparent data-[state=active]:border-b-2 data-[state=active]:border-primary rounded-none">Recent Trades</TabsTrigger>
                </TabsList>
              </div>
              <TabsContent value="holdings" className="flex-1 p-0 m-0 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Symbol</TableHead>
                      <TableHead>Qty</TableHead>
                      <TableHead>Avg Price</TableHead>
                      <TableHead>LTP</TableHead>
                      <TableHead>P&amp;L</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {portfolio?.positions?.map((pos) => (
                      <TableRow key={pos.symbol}>
                        <TableCell className="font-medium">{pos.symbol}</TableCell>
                        <TableCell>{formatQuantity(pos.quantity)}</TableCell>
                        <TableCell>--</TableCell>
                        <TableCell>--</TableCell>
                        <TableCell className="text-primary">+--</TableCell>
                      </TableRow>
                    ))}
                    {!portfolio?.positions?.length && (
                      <TableRow><TableCell colSpan={5} className="text-center py-8 text-muted-foreground">No current holdings</TableCell></TableRow>
                    )}
                  </TableBody>
                </Table>
              </TabsContent>
              <TabsContent value="orders" className="flex-1 p-0 m-0 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Side</TableHead>
                      <TableHead>Price</TableHead>
                      <TableHead>Remaining</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead className="text-right">Action</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {openOrders.length === 0 && (
                      <TableRow><TableCell colSpan={5} className="text-center text-muted-foreground py-6">No open orders</TableCell></TableRow>
                    )}
                    {openOrders.map((o) => (
                      <TableRow key={o.id}>
                        <TableCell className={o.side === "BUY" ? "text-primary font-medium" : "text-destructive font-medium"}>{o.side}</TableCell>
                        <TableCell>{formatMoney(o.price)}</TableCell>
                        <TableCell>{formatQuantity(o.remaining_quantity)}</TableCell>
                        <TableCell>{o.status}</TableCell>
                        <TableCell className="text-right">
                          <Button variant="ghost" size="sm" className="text-destructive h-8" onClick={() => cancelMutation.mutate(o.id)}>Cancel</Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TabsContent>
              <TabsContent value="trades" className="flex-1 p-0 m-0 overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Time</TableHead>
                      <TableHead>Price</TableHead>
                      <TableHead className="text-right">Quantity</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {trades.map((t) => (
                      <TableRow key={t.id}>
                        <TableCell className="text-muted-foreground text-xs">{new Date(t.created_at).toLocaleTimeString()}</TableCell>
                        <TableCell className="font-medium">{formatMoney(t.price)}</TableCell>
                        <TableCell className="text-right">{formatQuantity(t.quantity)}</TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </TabsContent>
            </Tabs>
          </Card>
        </div>

        {/* RIGHT COLUMN: Watchlist & Order Entry */}
        <div className="col-span-12 xl:col-span-4 flex flex-col gap-4">
          
          {/* Watchlist */}
          <Card className="flex flex-col max-h-[350px]">
            <CardHeader className="py-3 px-4 border-b flex flex-row items-center justify-between">
              <CardTitle className="text-sm font-semibold">Watchlist</CardTitle>
              <span className="text-primary text-xs cursor-pointer">View All +</span>
            </CardHeader>
            <CardContent className="flex-1 overflow-auto p-0">
              <Table>
                <TableHeader className="bg-muted/50 sticky top-0">
                  <TableRow>
                    <TableHead className="h-8 py-1">Symbol</TableHead>
                    <TableHead className="h-8 py-1 text-right">Last</TableHead>
                    <TableHead className="h-8 py-1 text-right">Chg%</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {watchlistItems.map((item) => (
                    <TableRow key={item.symbol} className="cursor-pointer hover:bg-muted" onClick={() => setSelectedSymbol(item.symbol.split('/')[0])}>
                      <TableCell className="font-medium py-2">{item.symbol}</TableCell>
                      <TableCell className="text-right py-2">{item.last}</TableCell>
                      <TableCell className={`text-right py-2 ${item.up ? 'text-primary' : 'text-destructive'}`}>{item.chgPct}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          {/* Order Entry */}
          <Card className="flex-1 flex flex-col">
            <CardHeader className="py-0 px-0 border-b">
              <Tabs value={side} onValueChange={(v) => setSide(v as Side)} className="w-full">
                <TabsList className="w-full grid grid-cols-2 h-12 rounded-none bg-transparent">
                  <TabsTrigger value="BUY" className="data-[state=active]:bg-primary/10 data-[state=active]:text-primary data-[state=active]:border-b-2 data-[state=active]:border-primary rounded-none transition-all text-sm font-semibold">Buy</TabsTrigger>
                  <TabsTrigger value="SELL" className="data-[state=active]:bg-destructive/10 data-[state=active]:text-destructive data-[state=active]:border-b-2 data-[state=active]:border-destructive rounded-none transition-all text-sm font-semibold">Sell</TabsTrigger>
                </TabsList>
              </Tabs>
            </CardHeader>
            <CardContent className="p-4 space-y-4">
              <div className="flex gap-2">
                {orderTypes.map((t) => (
                  <Button 
                    key={t}
                    variant={type === t ? "default" : "outline"}
                    size="sm"
                    className={cn("flex-1 text-xs h-8", type === t && side === "BUY" ? "bg-primary text-primary-foreground hover:bg-primary/90" : type === t && side === "SELL" ? "bg-destructive text-destructive-foreground hover:bg-destructive/90" : "")}
                    onClick={() => setType(t)}
                  >
                    {t}
                  </Button>
                ))}
              </div>

              <div className="space-y-4">
                <div className="space-y-2">
                  <Label className="text-xs text-muted-foreground">Symbol</Label>
                  <Input value={selectedSymbol} readOnly className="bg-muted/50 border-border h-9" />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label className="text-xs text-muted-foreground">Quantity</Label>
                    <Input type="number" value={quantity} onChange={(e) => setQuantity(e.target.value)} className="bg-muted/50 border-border h-9" />
                  </div>
                  <div className="space-y-2">
                    <Label className="text-xs text-muted-foreground">Price</Label>
                    <Input type="number" value={price} onChange={(e) => setPrice(e.target.value)} disabled={type === "MARKET"} className="bg-muted/50 border-border h-9" />
                  </div>
                </div>
                
                <div className="flex items-center justify-between pt-2">
                  <span className="text-xs text-muted-foreground">Available Margin</span>
                  <span className="text-sm font-semibold">{formatMoney(portfolio?.balance)}</span>
                </div>

                <Button 
                  type="button" 
                  onClick={onSubmitOrder}
                  className={cn("w-full h-10 font-bold", side === "BUY" ? "bg-primary hover:bg-primary/90 text-background" : "bg-destructive hover:bg-destructive/90 text-foreground")}
                  disabled={orderMutation.isPending}
                >
                  {orderMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  {side} {selectedSymbol}
                </Button>
              </div>
            </CardContent>
          </Card>

        </div>
      </div>
    </div>
  );
}
