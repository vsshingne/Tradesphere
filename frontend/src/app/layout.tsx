import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

import Providers from "@/components/providers";
import { Toaster } from "@/components/ui/sonner";
import RouteGuard from "@/components/route-guard";

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
});

export const metadata: Metadata = {
  title: "TradeSphere",
  description: "Professional Trading Platform",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body
        className={`${inter.variable} font-sans antialiased bg-background text-foreground`}
      >
        <Providers>
          <RouteGuard>
            {children}
          </RouteGuard>
          <Toaster />
        </Providers>
      </body>
    </html>
  );
}
