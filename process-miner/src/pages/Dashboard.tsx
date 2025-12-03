import React from 'react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useQuery } from '@tanstack/react-query';
import { base44 } from '@/api/base44Client';
import {
    ArrowRight,
    Layers,
    GitBranch,
    Workflow,
    ShieldAlert,
    Plus,
    TrendingUp,
    Activity,
    Clock,
    CheckCircle2,
    Wand2
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';

export default function Dashboard() {
    const { data: useCases = [] } = useQuery({
        queryKey: ['useCases'],
        queryFn: () => base44.entities.UseCase.list(),
    });

    const stats = [
        {
            label: 'Active Use Cases',
            value: useCases.filter(u => u.status === 'active').length || 0,
            icon: Layers,
            color: 'text-blue-600',
            bgColor: 'bg-blue-50'
        },
        {
            label: 'Process Variants',
            value: 8,
            icon: GitBranch,
            color: 'text-emerald-600',
            bgColor: 'bg-emerald-50'
        },
        {
            label: 'Discovered Processes',
            value: 1,
            icon: Workflow,
            color: 'text-violet-600',
            bgColor: 'bg-violet-50'
        },
        {
            label: 'Control Violations',
            value: 5,
            icon: ShieldAlert,
            color: 'text-amber-600',
            bgColor: 'bg-amber-50'
        },
    ];

    const quickActions = [
        {
            title: 'Start New Use Case',
            description: 'Onboard a new process mining use case',
            icon: Plus,
            page: 'Onboarding',
            color: 'bg-gradient-to-br from-blue-600 to-indigo-600'
        },
        {
            title: 'Discover Processes',
            description: 'Analyze and visualize process flows',
            icon: Workflow,
            page: 'ProcessDiscovery',
            color: 'bg-gradient-to-br from-emerald-600 to-teal-600'
        },
        {
            title: 'Manage Variants',
            description: 'Group and analyze process variants',
            icon: GitBranch,
            page: 'VariantManagement',
            color: 'bg-gradient-to-br from-violet-600 to-purple-600'
        },
        {
            title: 'Risk & Controls',
            description: 'Monitor risks and remediation',
            icon: ShieldAlert,
            page: 'RiskControls',
            color: 'bg-gradient-to-br from-amber-600 to-orange-600'
        },
    ];

    const recentActivities = [
        { action: 'New variant discovered in Credit Increase', time: '2 minutes ago', status: 'new' },
        { action: 'Control violation remediated', time: '15 minutes ago', status: 'success' },
        { action: 'Application onboarded: Risk Management', time: '1 hour ago', status: 'success' },
        { action: 'AI suggested 3 activity renames', time: '2 hours ago', status: 'pending' },
    ];

    return (
        <div className="space-y-8">
            {/* Header */}
            <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                <div>
                    <h1 className="text-3xl font-bold text-slate-900">Welcome to ProcessMind</h1>
                    <p className="text-slate-500 mt-1">Your enterprise process intelligence platform</p>
                </div>
                <div className="flex gap-2">
                    <Link to={createPageUrl('Onboarding')}>
                        <Button variant="outline" className="gap-2 border-violet-200 text-violet-700 hover:bg-violet-50">
                            <Wand2 className="w-4 h-4" />
                            Demo: Credit Increase
                        </Button>
                    </Link>
                    <Link to={createPageUrl('Onboarding')}>
                        <Button variant="outline" className="gap-2 border-amber-200 text-amber-700 hover:bg-amber-50">
                            <ShieldAlert className="w-4 h-4" />
                            Demo: Monitor Agent
                        </Button>
                    </Link>
                    <Link to={createPageUrl('Onboarding')}>
                        <Button className="bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700 shadow-lg shadow-blue-600/20">
                            <Plus className="w-4 h-4 mr-2" />
                            New Use Case
                        </Button>
                    </Link>
                </div>
            </div>

            {/* Stats Grid */}
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
                {stats.map((stat, index) => (
                    <Card key={index} className="border-0 shadow-lg shadow-slate-200/50 overflow-hidden">
                        <CardContent className="p-6">
                            <div className="flex items-start justify-between">
                                <div className={`p-3 rounded-xl ${stat.bgColor}`}>
                                    <stat.icon className={`w-6 h-6 ${stat.color}`} />
                                </div>
                                <TrendingUp className="w-4 h-4 text-emerald-500" />
                            </div>
                            <div className="mt-4">
                                <p className="text-3xl font-bold text-slate-900">{stat.value}</p>
                                <p className="text-sm text-slate-500 mt-1">{stat.label}</p>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>

            {/* Quick Actions */}
            <div>
                <h2 className="text-xl font-semibold text-slate-900 mb-4">Quick Actions</h2>
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                    {quickActions.map((action, index) => (
                        <Link key={index} to={createPageUrl(action.page)}>
                            <Card className="border-0 shadow-lg shadow-slate-200/50 hover:shadow-xl hover:-translate-y-1 transition-all duration-300 cursor-pointer group h-full">
                                <CardContent className="p-6">
                                    <div className={`w-12 h-12 rounded-xl ${action.color} flex items-center justify-center mb-4 shadow-lg`}>
                                        <action.icon className="w-6 h-6 text-white" />
                                    </div>
                                    <h3 className="font-semibold text-slate-900 group-hover:text-blue-600 transition-colors">
                                        {action.title}
                                    </h3>
                                    <p className="text-sm text-slate-500 mt-1">{action.description}</p>
                                    <ArrowRight className="w-4 h-4 text-slate-400 mt-4 group-hover:text-blue-600 group-hover:translate-x-1 transition-all" />
                                </CardContent>
                            </Card>
                        </Link>
                    ))}
                </div>
            </div>

            {/* Recent Activity & Use Cases */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Recent Activity */}
                <Card className="border-0 shadow-lg shadow-slate-200/50">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Activity className="w-5 h-5 text-blue-600" />
                            Recent Activity
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="space-y-4">
                            {recentActivities.map((activity, index) => (
                                <div key={index} className="flex items-center gap-4 p-3 rounded-xl bg-slate-50/50 hover:bg-slate-100/50 transition-colors">
                                    <div className={`w-2 h-2 rounded-full ${activity.status === 'new' ? 'bg-blue-500' :
                                        activity.status === 'success' ? 'bg-emerald-500' :
                                            'bg-amber-500'
                                        }`} />
                                    <div className="flex-1 min-w-0">
                                        <p className="text-sm font-medium text-slate-900 truncate">{activity.action}</p>
                                        <p className="text-xs text-slate-500 flex items-center gap-1">
                                            <Clock className="w-3 h-3" />
                                            {activity.time}
                                        </p>
                                    </div>
                                    {activity.status === 'success' && (
                                        <CheckCircle2 className="w-4 h-4 text-emerald-500 flex-shrink-0" />
                                    )}
                                    {activity.status === 'new' && (
                                        <Badge className="bg-blue-100 text-blue-700 border-0">New</Badge>
                                    )}
                                    {activity.status === 'pending' && (
                                        <Badge className="bg-amber-100 text-amber-700 border-0">Review</Badge>
                                    )}
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>

                {/* Active Use Cases */}
                <Card className="border-0 shadow-lg shadow-slate-200/50">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Layers className="w-5 h-5 text-violet-600" />
                            Active Use Cases
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        {useCases.length === 0 ? (
                            <div className="text-center py-8">
                                <div className="w-16 h-16 rounded-full bg-slate-100 flex items-center justify-center mx-auto mb-4">
                                    <Layers className="w-8 h-8 text-slate-400" />
                                </div>
                                <p className="text-slate-500 mb-4">No use cases yet</p>
                                <Link to={createPageUrl('Onboarding')}>
                                    <Button variant="outline" className="gap-2">
                                        <Plus className="w-4 h-4" />
                                        Create your first use case
                                    </Button>
                                </Link>
                            </div>
                        ) : (
                            <div className="space-y-3">
                                {useCases.slice(0, 4).map((useCase) => (
                                    <div key={useCase.id} className="flex items-center gap-4 p-4 rounded-xl bg-slate-50/50 hover:bg-slate-100/50 transition-colors">
                                        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-500 to-purple-600 flex items-center justify-center text-white font-semibold">
                                            {useCase.name?.charAt(0) || 'U'}
                                        </div>
                                        <div className="flex-1 min-w-0">
                                            <p className="font-medium text-slate-900 truncate">{useCase.name}</p>
                                            <p className="text-xs text-slate-500">{useCase.prc_group || 'No PRC group'}</p>
                                        </div>
                                        <Badge className={`border-0 ${useCase.status === 'active' ? 'bg-emerald-100 text-emerald-700' :
                                            useCase.status === 'completed' ? 'bg-blue-100 text-blue-700' :
                                                'bg-slate-100 text-slate-700'
                                            }`}>
                                            {useCase.status || 'Draft'}
                                        </Badge>
                                    </div>
                                ))}
                            </div>
                        )}
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}