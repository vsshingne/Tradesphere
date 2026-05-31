import Sidebar from "@/components/sidebar";
import TopHeader from "@/components/top-header";

export default function AppLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <div className="flex min-h-screen w-full bg-background">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0">
        <TopHeader />
        <main className="flex-1 overflow-auto ml-64 p-6 bg-[#0b1118]">
          {children}
        </main>
      </div>
    </div>
  );
}
