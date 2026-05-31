"use client";

import { useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useAuthStore } from "@/lib/store";

const publicRoutes = ["/login", "/signup", "/"];

export default function RouteGuard({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const token = useAuthStore((state) => state.token);
  const [authorized, setAuthorized] = useState(false);

  useEffect(() => {
    authCheck(pathname);
  }, [pathname, token]);

  function authCheck(url: string) {
    const isPublicRoute = publicRoutes.includes(url);
    if (!token && !isPublicRoute) {
      setAuthorized(false);
      router.push("/login");
    } else if (token && isPublicRoute) {
      setAuthorized(false);
      router.push("/dashboard");
    } else {
      setAuthorized(true);
    }
  }

  return authorized ? <>{children}</> : null;
}
