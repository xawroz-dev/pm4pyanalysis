// @ts-nocheck
import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue
} from '@/components/ui/select';

import {
    GitBranch,
    ArrowRight,
    Filter,
    TrendingUp,
    Clock,
    Users,
    AlertTriangle,
    ChevronDown,
    Wand2,
    BarChart3,
    PieChart,
    Eye,
    Workflow
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart as RechartsPie, Pie, Cell } from 'recharts';
import { useDemo } from '@/context/DemoContext';

// Activity colors
const activityColors = {
    'Request': 'bg-blue-50 text-blue-700 border border-blue-100',
    'Validate': 'bg-amber-50 text-amber-700 border border-amber-100',
    'Escalate': 'bg-orange-50 text-orange-700 border border-orange-100',
    'Review': 'bg-purple-50 text-purple-700 border border-purple-100',
    'Onboard': 'bg-teal-50 text-teal-700 border border-teal-100',
    'Close': 'bg-slate-50 text-slate-700 border border-slate-100',
    'Create Order': 'bg-emerald-50 text-emerald-700 border border-emerald-100',
    'Pick': 'bg-cyan-50 text-cyan-700 border border-cyan-100',
    'Pack': 'bg-indigo-50 text-indigo-700 border border-indigo-100',
    'Ship': 'bg-violet-50 text-violet-700 border border-violet-100',
    'Invoice': 'bg-rose-50 text-rose-700 border border-rose-100',
    'Receive': 'bg-lime-50 text-lime-700 border border-lime-100',
    'Approve': 'bg-green-50 text-green-700 border border-green-100',
    'Enrich': 'bg-yellow-50 text-yellow-700 border border-yellow-100',
    'Pay': 'bg-pink-50 text-pink-700 border border-pink-100',
    'Submit': 'bg-blue-50 text-blue-700 border border-blue-100',
    'Credit Check': 'bg-orange-50 text-orange-700 border border-orange-100',
    'Risk Assessment': 'bg-red-50 text-red-700 border border-red-100',
    'Manager Approval': 'bg-emerald-50 text-emerald-700 border border-emerald-100',
    'Senior Approval': 'bg-teal-50 text-teal-700 border border-teal-100',
    'Set Limit': 'bg-violet-50 text-violet-700 border border-violet-100',
    'Notify': 'bg-cyan-50 text-cyan-700 border border-cyan-100',
    'Deny': 'bg-red-50 text-red-700 border border-red-100',

    // Monitor Agent Colors (Comprehensive)
    'Triage': 'bg-indigo-50 text-indigo-700 border border-indigo-100',
    'Triage Agent': 'bg-indigo-50 text-indigo-700 border border-indigo-100',
    'Triage Request': 'bg-indigo-50 text-indigo-700 border border-indigo-100',
    'Triage: Analyze': 'bg-indigo-50 text-indigo-700 border border-indigo-100',

    'Identity': 'bg-fuchsia-50 text-fuchsia-700 border border-fuchsia-100',
    'Identity Agent': 'bg-fuchsia-50 text-fuchsia-700 border border-fuchsia-100',
    'Verify Identity': 'bg-fuchsia-50 text-fuchsia-700 border border-fuchsia-100',
    'Identity: Verify': 'bg-fuchsia-50 text-fuchsia-700 border border-fuchsia-100',

    'System Update': 'bg-sky-50 text-sky-700 border border-sky-100',
    'System Agent': 'bg-sky-50 text-sky-700 border border-sky-100',
    'Update CRM': 'bg-sky-50 text-sky-700 border border-sky-100',
    'System: Update DB': 'bg-sky-50 text-sky-700 border border-sky-100',

    'Card Issue': 'bg-emerald-50 text-emerald-700 border border-emerald-100',
    'Card Agent': 'bg-emerald-50 text-emerald-700 border border-emerald-100',
    'Order New Card': 'bg-emerald-50 text-emerald-700 border border-emerald-100',
    'Card Agent: Issue Card': 'bg-emerald-50 text-emerald-700 border border-emerald-100',

    'Notify': 'bg-cyan-50 text-cyan-700 border border-cyan-100',
    'Notify Agent': 'bg-cyan-50 text-cyan-700 border border-cyan-100',
    'Notify Customer': 'bg-cyan-50 text-cyan-700 border border-cyan-100',
    'Notify Agent: Send Conf': 'bg-cyan-50 text-cyan-700 border border-cyan-100',

    'Escalate': 'bg-red-50 text-red-700 border border-red-100',
    'Triage Agent: Escalate': 'bg-red-50 text-red-700 border border-red-100',
};

export default function VariantManagement() {
    const { data: demoData } = useDemo();
    const [variantGroups, setVariantGroups] = useState([]);
    const [expandedGroups, setExpandedGroups] = useState({ 0: true, 1: true, 2: true });
    const [selectedVariant, setSelectedVariant] = useState(null);
    const [filterApp, setFilterApp] = useState('all');
    const [filterSort, setFilterSort] = useState('frequency');
    const [throughputData, setThroughputData] = useState([]);

    useEffect(() => {
        if (demoData?.variantGroups) {
            setVariantGroups(demoData.variantGroups);
            // Expand first group by default
            setExpandedGroups({ 0: true });
            setSelectedVariant(null); // Reset selection on demo switch
        }
    }, [demoData]);

    // Generate random throughput data when variant is selected
    useEffect(() => {
        if (selectedVariant) {
            const stages = selectedVariant.activities.slice(0, 5); // Take first 5 activities for chart
            const newData = stages.map(stage => ({
                stage: stage.length > 10 ? stage.substring(0, 8) + '...' : stage,
                time: Math.floor(Math.random() * 15) + 2 // Random time between 2 and 17
            }));
            setThroughputData(newData);
        }
    }, [selectedVariant]);

    const toggleGroup = (index) => {
        setExpandedGroups(prev => ({ ...prev, [index]: !prev[index] }));
    };

    const totalCases = variantGroups.reduce((sum, g) => sum + g.totalCases, 0);
    const totalVariants = variantGroups.reduce((sum, g) => sum + g.variants.length, 0);
    const newVariants = variantGroups.flatMap(g => g.variants).filter(v => v.isNew).length;

    const renderVariantFlow = (activities) => (
        <div className="flex items-center gap-1 flex-wrap">
            {activities.map((activity, index) => (
                <React.Fragment key={index}>
                    <div className={cn(
                        "px-2.5 py-1 rounded-md text-xs font-medium shadow-sm",
                        activityColors[activity] || 'bg-slate-50 text-slate-700'
                    )}>
                        {activity}
                    </div>
                    {index < activities.length - 1 && (
                        <ArrowRight className="w-3.5 h-3.5 text-slate-400 flex-shrink-0" />
                    )}
                </React.Fragment>
            ))}
        </div>
    );

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-slate-900">Variant Management</h1>
                    <p className="text-slate-500">Analyze and manage process variants across your use cases</p>
                </div>
                <div className="flex items-center gap-2">
                    <Button variant="outline" className="gap-2">
                        <Wand2 className="w-4 h-4" />
                        Load Demo Data
                    </Button>
                    <Button className="gap-2 bg-gradient-to-r from-blue-600 to-indigo-600">
                        <Workflow className="w-4 h-4" />
                        Generate Group BPMN
                    </Button>
                </div>
            </div>

            {/* Filters */}
            <Card className="border-0 shadow-lg shadow-slate-200/50">
                <CardContent className="p-4">
                    <div className="flex flex-wrap items-center gap-4">
                        <div className="flex items-center gap-2">
                            <Filter className="w-4 h-4 text-slate-500" />
                            <span className="text-sm font-medium text-slate-700">Filters:</span>
                        </div>
                        <Select value={filterApp} onValueChange={setFilterApp}>
                            <SelectTrigger className="w-48">
                                <SelectValue placeholder="Application" />
                            </SelectTrigger>
                            <SelectContent>
                                <SelectItem value="all">All Applications</SelectItem>
                                <SelectItem value="core_banking">Core Banking System</SelectItem>
                                <SelectItem value="crm">CRM System</SelectItem>
                                <SelectItem value="risk">Risk Management</SelectItem>
                            </SelectContent>
                        </Select>
                        <Select value={filterSort} onValueChange={setFilterSort}>
                            <SelectTrigger className="w-48">
                                <SelectValue placeholder="Sort by" />
                            </SelectTrigger>
                            <SelectContent>
                                <SelectItem value="frequency">Frequency ↓</SelectItem>
                                <SelectItem value="duration">Duration ↓</SelectItem>
                                <SelectItem value="name">Name A-Z</SelectItem>
                            </SelectContent>
                        </Select>
                        <div className="flex gap-2 ml-auto">
                            <Button variant="outline" size="sm">Apply</Button>
                            <Button variant="ghost" size="sm">Reset</Button>
                        </div>
                    </div>
                </CardContent>
            </Card>

            <div className="grid lg:grid-cols-3 gap-6">
                {/* Left: Variants & Groups */}
                <div className="lg:col-span-2 space-y-4">
                    <Card className="border-0 shadow-xl shadow-slate-200/50">
                        <CardHeader className="pb-2">
                            <CardTitle className="flex items-center justify-between">
                                <span className="flex items-center gap-2">
                                    <GitBranch className="w-5 h-5 text-blue-600" />
                                    Variants & Groups
                                </span>
                                <div className="flex items-center gap-2 text-sm font-normal">
                                    <Badge className="bg-blue-100 text-blue-700 border-0">{totalVariants} variants</Badge>
                                    <Badge className="bg-emerald-100 text-emerald-700 border-0">{totalCases.toLocaleString()} cases</Badge>
                                </div>
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            {variantGroups.map((group, groupIndex) => (
                                <div key={groupIndex} className="rounded-xl border border-slate-200 overflow-hidden">
                                    {/* Group Header */}
                                    <button
                                        onClick={() => toggleGroup(groupIndex)}
                                        className="w-full flex items-center justify-between p-4 bg-gradient-to-r from-slate-50 to-white hover:from-slate-100 hover:to-slate-50 transition-colors"
                                    >
                                        <div className="flex items-center gap-3">
                                            <ChevronDown className={cn(
                                                "w-5 h-5 text-slate-400 transition-transform",
                                                !expandedGroups[groupIndex] && "-rotate-90"
                                            )} />
                                            <div className="text-left">
                                                <h3 className="font-semibold text-slate-900">{group.name}</h3>
                                                <p className="text-sm text-slate-500">
                                                    {group.variants.length} variants • {group.totalCases.toLocaleString()} cases
                                                </p>
                                            </div>
                                        </div>
                                        <Badge variant="outline" className="font-mono">
                                            {((group.totalCases / totalCases) * 100).toFixed(1)}%
                                        </Badge>
                                    </button>

                                    {/* Variants */}
                                    {expandedGroups[groupIndex] && (
                                        <div className="border-t border-slate-200">
                                            {group.variants.map((variant, variantIndex) => (
                                                <div
                                                    key={variant.id}
                                                    onClick={() => setSelectedVariant(variant)}
                                                    className={cn(
                                                        "p-4 border-b border-slate-100 last:border-b-0 hover:bg-slate-50/50 transition-colors cursor-pointer",
                                                        variant.isNew && "bg-amber-50/50",
                                                        selectedVariant?.id === variant.id && "ring-2 ring-blue-400 bg-blue-50/50"
                                                    )}
                                                >
                                                    <div className="flex items-start justify-between gap-4 mb-3">
                                                        <div className="flex items-center gap-3">
                                                            <span className="font-mono font-bold text-slate-700">{variant.id}</span>
                                                            {variant.isNew && (
                                                                <Badge className="bg-amber-100 text-amber-700 border-0">
                                                                    <AlertTriangle className="w-3 h-3 mr-1" />
                                                                    New
                                                                </Badge>
                                                            )}
                                                        </div>
                                                        <div className="flex items-center gap-4 text-sm text-slate-500">
                                                            <span className="flex items-center gap-1">
                                                                <Users className="w-3.5 h-3.5" />
                                                                {variant.caseCount.toLocaleString()}
                                                            </span>
                                                            <span className="flex items-center gap-1">
                                                                <TrendingUp className="w-3.5 h-3.5" />
                                                                {variant.percentage}%
                                                            </span>
                                                            <span className="flex items-center gap-1">
                                                                <Clock className="w-3.5 h-3.5" />
                                                                {variant.avgDuration}
                                                            </span>
                                                        </div>
                                                    </div>
                                                    {renderVariantFlow(variant.activities)}
                                                </div>
                                            ))}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </CardContent>
                    </Card>
                </div>

                {/* Right: Variant Details Panel */}
                <div className="space-y-4">
                    <Card className="border-0 shadow-xl shadow-slate-200/50">
                        <CardHeader className="pb-2">
                            <CardTitle className="text-sm flex items-center gap-2">
                                <BarChart3 className="w-4 h-4 text-blue-600" />
                                {selectedVariant ? `Variant ${selectedVariant.id}` : 'Select a Variant'}
                            </CardTitle>
                        </CardHeader>
                        <CardContent>
                            {selectedVariant ? (
                                <div className="space-y-4">
                                    {/* Variant Flow */}
                                    <div>
                                        <p className="text-xs font-medium text-slate-500 mb-2">Activity Flow</p>
                                        <div className="flex flex-wrap gap-1">
                                            {selectedVariant.activities.map((activity, index) => (
                                                <React.Fragment key={index}>
                                                    <div className={cn(
                                                        "px-2 py-1 rounded text-xs font-medium",
                                                        activityColors[activity] || 'bg-slate-50 text-slate-700'
                                                    )}>
                                                        {activity}
                                                    </div>
                                                    {index < selectedVariant.activities.length - 1 && (
                                                        <ArrowRight className="w-3 h-3 text-slate-400 self-center" />
                                                    )}
                                                </React.Fragment>
                                            ))}
                                        </div>
                                    </div>

                                    {/* Metrics Grid */}
                                    <div className="grid grid-cols-2 gap-4">
                                        <div className="p-3 bg-slate-50 rounded-lg">
                                            <p className="text-xs text-slate-500 mb-1">Total Cases</p>
                                            <p className="text-lg font-semibold text-slate-900">{selectedVariant.caseCount.toLocaleString()}</p>
                                        </div>
                                        <div className="p-3 bg-slate-50 rounded-lg">
                                            <p className="text-xs text-slate-500 mb-1">Avg Duration</p>
                                            <p className="text-lg font-semibold text-slate-900">{selectedVariant.avgDuration}</p>
                                        </div>
                                    </div>

                                    {/* Throughput Analysis Chart */}
                                    <div className="h-48">
                                        <p className="text-xs font-medium text-slate-500 mb-2">Throughput Analysis (Avg Duration)</p>
                                        <ResponsiveContainer width="100%" height="100%">
                                            <BarChart data={throughputData}>
                                                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                                                <XAxis dataKey="stage" tick={{ fontSize: 10 }} />
                                                <YAxis tick={{ fontSize: 10 }} />
                                                <Tooltip
                                                    contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 12px rgba(0,0,0,0.1)' }}
                                                    cursor={{ fill: '#f1f5f9' }}
                                                />
                                                <Bar dataKey="time" fill="#8b5cf6" radius={[4, 4, 0, 0]} barSize={30} />
                                            </BarChart>
                                        </ResponsiveContainer>
                                    </div>
                                </div>
                            ) : (
                                <div className="py-12 text-center">
                                    <div className="w-12 h-12 rounded-full bg-slate-100 flex items-center justify-center mx-auto mb-3">
                                        <Eye className="w-6 h-6 text-slate-400" />
                                    </div>
                                    <h3 className="text-sm font-medium text-slate-900">No Variant Selected</h3>
                                    <p className="text-xs text-slate-500 mt-1 max-w-[200px] mx-auto">
                                        Click on a variant from the list to view detailed analysis and metrics
                                    </p>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </div>
            </div>
        </div>
    );
}