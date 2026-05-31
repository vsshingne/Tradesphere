import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatMoney(amount: string | number | undefined): string {
  if (!amount) return "$0.00";
  const num = typeof amount === "string" ? parseFloat(amount) : amount;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
  }).format(num);
}

export function formatQuantity(amount: string | number | undefined): string {
  if (!amount) return "0.0000";
  const num = typeof amount === "string" ? parseFloat(amount) : amount;
  return num.toFixed(4);
}
