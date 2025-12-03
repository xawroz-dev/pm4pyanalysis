import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from './utils';
import {
    LayoutDashboard,
    Settings,
    GitBranch,
    Layers,
    Workflow,
    ShieldAlert,
    ChevronLeft,
    ChevronRight,
    Sparkles,
    Menu,
    X
} from 'lucide-react';
import { cn } from '@/lib/utils';

const navigation = [
    { name: 'Dashboard', page: 'Dashboard', icon: LayoutDashboard },
    { name: 'Onboarding', page: 'Onboarding', icon: Settings },
    { name: 'Process Discovery', page: 'ProcessDiscovery', icon: Workflow },
    { name: 'Variant Management', page: 'VariantManagement', icon: GitBranch },
    { name: 'Risk & Controls', page: 'RiskControls', icon: ShieldAlert },
];

export default function Layout({ children, currentPageName }) {
    const [collapsed, setCollapsed] = useState(false);
    const [mobileOpen, setMobileOpen] = useState(false);

    return (
        <div className="min-h-screen bg-gradient-to-br from-slate-50 via-white to-blue-50/30">
            {/* Mobile Header */}
            <div className="lg:hidden fixed top-0 left-0 right-0 h-16 bg-white/80 backdrop-blur-xl border-b border-slate-200/50 z-50 flex items-center px-4">
                <button
                    onClick={() => setMobileOpen(!mobileOpen)}
                    className="p-2 rounded-xl hover:bg-slate-100 transition-colors"
                >
                    {mobileOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
                </button>
                <div className="flex items-center gap-3 ml-4">
                    <div className="w-8 h-8 rounded-xl bg-gradient-to-br from-blue-600 to-indigo-600 flex items-center justify-center">
                        <Sparkles className="w-4 h-4 text-white" />
                    </div>
                    <span className="font-semibold text-slate-800">ProcessMind</span>
                </div>
            </div>

            {/* Mobile Sidebar Overlay */}
            {mobileOpen && (
                <div
                    className="lg:hidden fixed inset-0 bg-black/20 backdrop-blur-sm z-40"
                    onClick={() => setMobileOpen(false)}
                />
            )}

            {/* Sidebar */}
            <aside className={cn(
                "fixed top-0 left-0 h-full bg-white/70 backdrop-blur-xl border-r border-slate-200/50 z-50 transition-all duration-300 ease-out",
                collapsed ? "w-20" : "w-72",
                mobileOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0"
            )}>
                <div className="flex flex-col h-full">
                    {/* Logo */}
                    <div className={cn(
                        "h-20 flex items-center border-b border-slate-200/50 transition-all duration-300",
                        collapsed ? "px-4 justify-center" : "px-6"
                    )}>
                        <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-600 to-indigo-600 flex items-center justify-center shadow-lg shadow-blue-600/20">
                                <Sparkles className="w-5 h-5 text-white" />
                            </div>
                            {!collapsed && (
                                <div>
                                    <h1 className="font-bold text-lg text-slate-800">ProcessMind</h1>
                                    <p className="text-xs text-slate-500">Enterprise Edition</p>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Navigation */}
                    <nav className="flex-1 p-4 space-y-1">
                        {navigation.map((item) => {
                            const isActive = currentPageName === item.page;
                            const Icon = item.icon;
                            return (
                                <Link
                                    key={item.name}
                                    to={createPageUrl(item.page)}
                                    onClick={() => setMobileOpen(false)}
                                    className={cn(
                                        "flex items-center gap-3 px-4 py-3 rounded-xl transition-all duration-200",
                                        isActive
                                            ? "bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-lg shadow-blue-600/20"
                                            : "text-slate-600 hover:bg-slate-100/80 hover:text-slate-900",
                                        collapsed && "justify-center px-3"
                                    )}
                                >
                                    <Icon className={cn("w-5 h-5 flex-shrink-0", isActive && "text-white")} />
                                    {!collapsed && (
                                        <span className="font-medium">{item.name}</span>
                                    )}
                                </Link>
                            );
                        })}
                    </nav>

                    {/* Collapse Button */}
                    <div className="p-4 border-t border-slate-200/50 hidden lg:block">
                        <button
                            onClick={() => setCollapsed(!collapsed)}
                            className={cn(
                                "flex items-center gap-3 px-4 py-3 rounded-xl text-slate-500 hover:bg-slate-100/80 hover:text-slate-700 transition-all w-full",
                                collapsed && "justify-center px-3"
                            )}
                        >
                            {collapsed ? (
                                <ChevronRight className="w-5 h-5" />
                            ) : (
                                <>
                                    <ChevronLeft className="w-5 h-5" />
                                    <span className="font-medium">Collapse</span>
                                </>
                            )}
                        </button>
                    </div>
                </div>
            </aside>

            {/* Main Content */}
            <main className={cn(
                "transition-all duration-300 min-h-screen",
                collapsed ? "lg:ml-20" : "lg:ml-72",
                "pt-16 lg:pt-0"
            )}>
                <div className="p-6 lg:p-8">
                    {children}
                </div>
            </main>
        </div>
    );
}    