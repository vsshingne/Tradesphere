import { useEffect, useState, useRef } from "react";
import { useAuthStore } from "./store";

export type Side = "BUY" | "SELL";

export interface Trade {
  id: string;
  price: string;
  quantity: string;
  created_at: string;
}

export interface Order {
  id: string;
  side: Side;
  price: string;
  remaining_quantity: string;
  status: string;
}

export function useWebSocket() {
  const [isConnected, setIsConnected] = useState(false);
  const [trades, setTrades] = useState<Trade[]>([]);
  const [openOrders, setOpenOrders] = useState<Order[]>([]);
  const [bids, setBids] = useState<Record<string, string>>({});
  const [asks, setAsks] = useState<Record<string, string>>({});
  
  const wsRef = useRef<WebSocket | null>(null);
  const { user } = useAuthStore();

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = `${protocol}//${window.location.host}/ws`;
    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    ws.onopen = () => setIsConnected(true);
    ws.onclose = () => setIsConnected(false);
    
    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === "TRADE") {
          setTrades((prev) => [msg.data, ...prev].slice(0, 50));
        } else if (msg.type === "ORDER_UPDATE") {
          // If the order belongs to this user, update it
          if (msg.data.user_id === user?.id) {
            setOpenOrders((prev) => {
              const existing = prev.findIndex((o) => o.id === msg.data.id);
              if (msg.data.status === "FILLED" || msg.data.status === "CANCELLED" || msg.data.status === "REJECTED") {
                if (existing >= 0) return prev.filter((o) => o.id !== msg.data.id);
                return prev;
              }
              if (existing >= 0) {
                const next = [...prev];
                next[existing] = msg.data;
                return next;
              }
              return [msg.data, ...prev];
            });
          }
          
          // Update Orderbook (simplified for demo)
          if (msg.data.status === "NEW" || msg.data.status === "PARTIALLY_FILLED") {
             if (msg.data.side === "BUY") {
               setBids((prev) => ({ ...prev, [msg.data.price]: msg.data.remaining_quantity }));
             } else {
               setAsks((prev) => ({ ...prev, [msg.data.price]: msg.data.remaining_quantity }));
             }
          } else if (msg.data.status === "FILLED" || msg.data.status === "CANCELLED") {
             if (msg.data.side === "BUY") {
               setBids((prev) => {
                 const next = { ...prev };
                 delete next[msg.data.price];
                 return next;
               });
             } else {
               setAsks((prev) => {
                 const next = { ...prev };
                 delete next[msg.data.price];
                 return next;
               });
             }
          }
        }
      } catch (err) {
        console.error("Failed to parse WS message", err);
      }
    };

    return () => {
      ws.close();
    };
  }, [user?.id]);

  return { isConnected, trades, openOrders, bids, asks };
}
