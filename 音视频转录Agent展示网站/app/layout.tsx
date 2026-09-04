import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';

const geistSans = Geist({
  variable: '--font-geist-sans',
  subsets: ['latin'],
});

const geistMono = Geist_Mono({
  variable: '--font-geist-mono',
  subsets: ['latin'],
});

export const metadata: Metadata = {
  title: '音视频转录 Agent｜单场直播录制、转录与分析',
  description: '把一场抖音直播变成可回看、可检索、可分析的内容档案。公开静态展示，不连接真实 Agent 或 API。',
  openGraph: {
    title: '音视频转录 Agent',
    description: '单场直播录制、中文转录、保守清洗与内容分析。',
    type: 'website',
  },
  twitter: {
    card: 'summary',
    title: '音视频转录 Agent',
    description: '单场直播录制、中文转录、保守清洗与内容分析。',
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body
        className={`${geistSans.variable} ${geistMono.variable} antialiased`}
      >
        {children}
      </body>
    </html>
  );
}
