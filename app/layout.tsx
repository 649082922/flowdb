import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  metadataBase: new URL("https://flowdb-migration-console.wiggly-calf-8827.chatgpt.site"),
  title: "FlowDB · 数据库迁移平台",
  description: "支持 Oracle、MySQL 与 PostgreSQL 的在线数据库迁移控制台。",
  icons: { icon: "/favicon.svg", shortcut: "/favicon.svg" },
  openGraph: {
    title: "FlowDB · 数据库迁移平台",
    description: "在自己的服务器上完成 Oracle、MySQL 与 PostgreSQL 全量迁移。",
    images: [{ url: "/og.png", width: 1734, height: 907, alt: "FlowDB 数据库迁移平台" }],
  },
  twitter: { card: "summary_large_image", images: ["/og.png"] },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><body>{children}</body></html>;
}
