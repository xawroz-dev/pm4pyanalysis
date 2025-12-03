import React from 'react';
import { Link, useLocation } from 'react-router-dom';
import {
    LayoutDashboard,
    Settings,
    GitBranch,
    ShieldAlert,
    Workflow,
    ChevronLeft,
    Plus
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { Button } from '@/components/ui/button';

const sidebarItems = [
    { icon: LayoutDashboard, label: 'Dashboard', path: '/dashboard' },
    { icon: Settings, label: 'Onboarding', path: '/onboarding' },
    { icon: Workflow, label: 'Process Discovery', path: '/processdiscovery' },
    { icon: GitBranch, label: 'Variant Management', path: '/variantmanagement' },
    { icon: ShieldAlert, label: 'Risk & Controls', path: '/riskcontrols' },
];

export default function Layout({ children }: { children: React.ReactNode }) {
    const location = useLocation();
    const [collapsed, setCollapsed] = React.useState(false);

    return (
        <div className="flex min-h-screen bg-slate-50">
            {/* Sidebar */}
            <div className={cn(
                "bg-white border-r border-slate-200 flex flex-col transition-all duration-300 relative",
                collapsed ? "w-20" : "w-64"
            )}>
                {/* Logo */}
                <div className="p-6 flex items-center gap-3">
                    <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-600 to-indigo-600 flex items-center justify-center shrink-0">
                        <SparklesIcon className="w-5 h-5 text-white" />
                    </div>
                    {!collapsed && (
                        <div>
                            <h1 className="font-bold text-slate-900 leading-none">ProcessMind</h1>
                            <span className="text-xs text-slate-500">Enterprise Edition</span>
                        </div>
                    )}
                </div>

                {/* Navigation */}
                <nav className="flex-1 px-3 py-4 space-y-1">
                    {sidebarItems.map((item) => {
                        const isActive = location.pathname === item.path;
                        return (
                            <Link
                                key={item.path}
                                to={item.path}
                                className={cn(
                                    "flex items-center gap-3 px-3 py-2.5 rounded-lg transition-colors",
                                    isActive
                                        ? "bg-blue-600 text-white shadow-lg shadow-blue-200"
                                        : "text-slate-600 hover:bg-slate-50 hover:text-slate-900"
                                )}
                            >
                                <item.icon className={cn("w-5 h-5", isActive ? "text-white" : "text-slate-500")} />
                                {!collapsed && <span className="font-medium">{item.label}</span>}
                            </Link>
                        );
                    })}
                </nav>

                {/* Collapse Toggle */}
                <div className="p-4 border-t border-slate-100">
                    <button
                        onClick={() => setCollapsed(!collapsed)}
                        className="flex items-center gap-3 text-slate-500 hover:text-slate-900 w-full px-3 py-2 rounded-lg hover:bg-slate-50 transition-colors"
                    >
                        <ChevronLeft className={cn("w-5 h-5 transition-transform", collapsed && "rotate-180")} />
                        {!collapsed && <span className="font-medium">Collapse</span>}
                    </button>
                </div>
            </div>

            {/* Main Content */}
            <main className="flex-1 overflow-auto">
                <div className="p-8">
                    {children}
                </div>
            </main>
        </div>
    );
}

function SparklesIcon({ className }: { className?: string }) {
    return (
        <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            className={className}
        >
            <path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z" />
        </svg>
    )
}
