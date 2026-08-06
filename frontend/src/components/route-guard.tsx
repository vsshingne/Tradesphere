"use client";

import { useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useAuthStore } from "@/lib/store";

const publicRoutes = ["/login", "/signup", "/"];

export default function RouteGuard({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const token = useAuthStore((state) => state.token);

  const isPublicRoute = publicRoutes.includes(pathname);
  const isAuthorized = token ? !isPublicRoute : isPublicRoute;

  useEffect(() => {
    if (!token && !isPublicRoute) {
      router.push("/login");
    } else if (token && isPublicRoute) {
      router.push("/dashboard");
    }
  }, [token, isPublicRoute, router]);

  return isAuthorized ? <>{children}</> : null;
}
