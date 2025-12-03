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
    ShieldAlert,
    CheckCircle2,
    Sparkles,
    Shield,
    AlertCircle,
    XCircle,
    Clock,
    Zap,
    ThumbsUp,
    ThumbsDown,
    Eye,
    Filter,
    Wand2,
    ArrowRight,
    Loader2,
    Play,
    X,
    ChevronRight,
    AlertTriangle,
    Users
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, LineChart, Line } from 'recharts';
import { useDemo } from '@/context/DemoContext';

// Default fallback if no specific journey found
const defaultJourney = [
    { activity: 'Credit Request Submitted', timestamp: '2024-01-15 09:23:14', user: 'customer_portal', system: 'Core Banking', status: 'completed' },
    { activity: 'Initial Validation', timestamp: '2024-01-15 09:23:45', user: 'AUTO_SYSTEM', system: 'Core Banking', status: 'completed' },
    { activity: 'Credit Score Check', timestamp: '2024-01-15 09:24:02', user: 'AUTO_SYSTEM', system: 'Risk Management', status: 'completed' },
    { activity: 'Risk Assessment', timestamp: '2024-01-15 10:15:33', user: 'risk_analyst_01', system: 'Risk Management', status: 'completed' },
    { activity: 'Manager Approval', timestamp: '-', user: '-', system: '-', status: 'skipped', violation: true },
    { activity: 'Credit Limit Updated', timestamp: '2024-01-15 10:45:12', user: 'credit_officer_03', system: 'Core Banking', status: 'completed' },
    { activity: 'Customer Notified', timestamp: '2024-01-15 10:45:45', user: 'AUTO_SYSTEM', system: 'CRM', status: 'completed' },
];

// Chart data
const violationsBySeverity = [
    { name: 'Critical', value: 156, color: '#ef4444' },
    { name: 'High', value: 323, color: '#f97316' },
    { name: 'Medium', value: 67, color: '#eab308' },
    { name: 'Low', value: 312, color: '#3b82f6' },
];

const violationsTrend = [
    { month: 'Jul', violations: 450 },
    { month: 'Aug', violations: 420 },
    { month: 'Sep', violations: 480 },
    { month: 'Oct', violations: 520 },
    { month: 'Nov', violations: 490 },
    { month: 'Dec', violations: 545 },
    { month: 'Jan', violations: 580 },
];

const severityConfig = {
    critical: { color: 'bg-red-100 text-red-700 border-red-200', icon: XCircle, iconColor: 'text-red-500', bgLight: 'bg-red-50' },
    high: { color: 'bg-orange-100 text-orange-700 border-orange-200', icon: AlertTriangle, iconColor: 'text-orange-500', bgLight: 'bg-orange-50' },
    medium: { color: 'bg-amber-100 text-amber-700 border-amber-200', icon: AlertCircle, iconColor: 'text-amber-500', bgLight: 'bg-amber-50' },
    low: { color: 'bg-blue-100 text-blue-700 border-blue-200', icon: Shield, iconColor: 'text-blue-500', bgLight: 'bg-blue-50' },
};

const statusConfig = {
    identified: { color: 'bg-slate-100 text-slate-700', label: 'Identified' },
    pending_approval: { color: 'bg-amber-100 text-amber-700', label: 'Pending Approval' },
    mitigated: { color: 'bg-emerald-100 text-emerald-700', label: 'Mitigated' },
    remediated: { color: 'bg-blue-100 text-blue-700', label: 'Remediated' },
};

export default function RiskControl() {
    const { data: demoData } = useDemo();
    const [violations, setViolations] = useState([]);
    const [selectedViolation, setSelectedViolation] = useState(null);
    const [selectedCaseId, setSelectedCaseId] = useState(null);
    const [showRemediationModal, setShowRemediationModal] = useState(null);
    const [remediating, setRemediating] = useState(null);
    const [filterSeverity, setFilterSeverity] = useState('all');
    const [filterStatus, setFilterStatus] = useState('all');
    const [violationJourneys, setViolationJourneys] = useState({});

    useEffect(() => {
        console.log('RiskControl: demoData updated', demoData);
        if (demoData?.riskViolations) {
            setViolations(demoData.riskViolations);
            // Only set selected if not already set or if switching demos
            if (!selectedViolation || !demoData.riskViolations.find(v => v.id === selectedViolation.id)) {
                setSelectedViolation(demoData.riskViolations[0]);
            }
        } else {
            setViolations([]);
            setSelectedViolation(null);
        }

        if (demoData?.violationJourneys) {
            setViolationJourneys(demoData.violationJourneys);
        } else {
            setViolationJourneys({});
        }
    }, [demoData]);

    const handleRemediation = async (violation, autoRemediate) => {
        setRemediating(violation.id);
        await new Promise(resolve => setTimeout(resolve, 2000));

        setViolations(prev => prev.map(v =>
            v.id === violation.id
                ? { ...v, status: autoRemediate ? 'remediated' : 'pending_approval' }
                : v
        ));
        setRemediating(null);
        setShowRemediationModal(null);
    };

    const approveRemediation = (violationId) => {
        setViolations(prev => prev.map(v =>
            v.id === violationId ? { ...v, status: 'remediated' } : v
        ));
    };

    const stats = {
        total: violations.reduce((sum, v) => sum + v.violationCount, 0),
        critical: violations.filter(v => v.severity === 'critical').reduce((sum, v) => sum + v.violationCount, 0),
        open: violations.filter(v => v.status === 'identified' || v.status === 'pending_approval').length,
        remediated: violations.filter(v => v.status === 'remediated' || v.status === 'mitigated').length,
    };

    const filteredViolations = violations.filter(v => {
        const severityMatch = filterSeverity === 'all' || v.severity === filterSeverity;
        const statusMatch = filterStatus === 'all' || v.status === filterStatus;
        return severityMatch && statusMatch;
    });

    const getJourneySteps = () => {
        if (!selectedViolation) return [];
        const journeys = violationJourneys[selectedViolation.id];
        if (journeys && journeys.length > 0) {
            if (selectedCaseId) {
                return journeys.find(j => j.caseId === selectedCaseId)?.steps || journeys[0].steps;
            }
            return journeys[0].steps;
        }
        return defaultJourney;
    };

    const getAvailableCases = () => {
        if (!selectedViolation) return [];
        return violationJourneys[selectedViolation.id] || [];
    };

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-slate-900">Risk & Controls Dashboard</h1>
                    <p className="text-slate-500">Monitor control violations and remediation status</p>
                </div>
                <Button variant="outline" className="gap-2 border-violet-200 text-violet-700 hover:bg-violet-50">
                    <Wand2 className="w-4 h-4" />
                    Load Demo Data
                </Button>
            </div>

            {/* Stats Cards */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                <Card className="border-0 shadow-lg shadow-slate-200/50">
                    <CardContent className="p-4">
                        <div className="flex items-center gap-3">
                            <div className="p-2.5 rounded-xl bg-slate-100">
                                <ShieldAlert className="w-5 h-5 text-slate-600" />
                            </div>
                            <div>
                                <p className="text-2xl font-bold text-slate-900">{stats.total}</p>
                                <p className="text-sm text-slate-500">Total Violations</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card className="border-0 shadow-lg shadow-slate-200/50">
                    <CardContent className="p-4">
                        <div className="flex items-center gap-3">
                            <div className="p-2.5 rounded-xl bg-red-50">
                                <XCircle className="w-5 h-5 text-red-600" />
                            </div>
                            <div>
                                <p className="text-2xl font-bold text-slate-900">{stats.critical}</p>
                                <p className="text-sm text-slate-500">Critical</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card className="border-0 shadow-lg shadow-slate-200/50">
                    <CardContent className="p-4">
                        <div className="flex items-center gap-3">
                            <div className="p-2.5 rounded-xl bg-amber-50">
                                <AlertTriangle className="w-5 h-5 text-amber-600" />
                            </div>
                            <div>
                                <p className="text-2xl font-bold text-slate-900">{stats.open}</p>
                                <p className="text-sm text-slate-500">Open Issues</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
                <Card className="border-0 shadow-lg shadow-slate-200/50">
                    <CardContent className="p-4">
                        <div className="flex items-center gap-3">
                            <div className="p-2.5 rounded-xl bg-emerald-50">
                                <CheckCircle2 className="w-5 h-5 text-emerald-600" />
                            </div>
                            <div>
                                <p className="text-2xl font-bold text-slate-900">{stats.remediated}</p>
                                <p className="text-sm text-slate-500">Remediated</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Charts Row */}
            <div className="grid lg:grid-cols-3 gap-6">
                <Card className="border-0 shadow-xl shadow-slate-200/50">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-base">Violations by Severity</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-48">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                    <Pie
                                        data={violationsBySeverity}
                                        cx="50%"
                                        cy="50%"
                                        innerRadius={40}
                                        outerRadius={70}
                                        dataKey="value"
                                        label={({ name, percent }) => `${(percent * 100).toFixed(0)}%`}
                                        labelLine={false}
                                    >
                                        {violationsBySeverity.map((entry, index) => (
                                            <Cell key={index} fill={entry.color} />
                                        ))}
                                    </Pie>
                                    <Tooltip />
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                        <div className="flex justify-center gap-3 text-xs mt-2">
                            {violationsBySeverity.map((item, i) => (
                                <div key={i} className="flex items-center gap-1">
                                    <div className="w-2.5 h-2.5 rounded" style={{ backgroundColor: item.color }} />
                                    {item.name}
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>

                <Card className="border-0 shadow-xl shadow-slate-200/50 lg:col-span-2">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-base">Violation Trend (Last 7 Months)</CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="h-48">
                            <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={violationsTrend}>
                                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                                    <XAxis dataKey="month" tick={{ fontSize: 12 }} />
                                    <YAxis tick={{ fontSize: 12 }} />
                                    <Tooltip />
                                    <Line type="monotone" dataKey="violations" stroke="#8b5cf6" strokeWidth={2} dot={{ fill: '#8b5cf6' }} />
                                </LineChart>
                            </ResponsiveContainer>
                        </div>
                    </CardContent>
                </Card>
            </div>

            {/* Violations Table */}
            <Card className="border-0 shadow-xl shadow-slate-200/50">
                <CardHeader className="pb-2">
                    <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
                        <CardTitle className="flex items-center gap-2">
                            <ShieldAlert className="w-5 h-5 text-red-500" />
                            Control Violations
                        </CardTitle>
                        <div className="flex items-center gap-2">
                            <Filter className="w-4 h-4 text-slate-500" />
                            <Select value={filterSeverity} onValueChange={setFilterSeverity}>
                                <SelectTrigger className="w-32">
                                    <SelectValue placeholder="Severity" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="all">All Severity</SelectItem>
                                    <SelectItem value="critical">Critical</SelectItem>
                                    <SelectItem value="high">High</SelectItem>
                                    <SelectItem value="medium">Medium</SelectItem>
                                    <SelectItem value="low">Low</SelectItem>
                                </SelectContent>
                            </Select>
                            <Select value={filterStatus} onValueChange={setFilterStatus}>
                                <SelectTrigger className="w-32">
                                    <SelectValue placeholder="Status" />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="all">All Status</SelectItem>
                                    <SelectItem value="identified">Identified</SelectItem>
                                    <SelectItem value="pending_approval">Pending Approval</SelectItem>
                                    <SelectItem value="mitigated">Mitigated</SelectItem>
                                    <SelectItem value="remediated">Remediated</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                    </div>
                </CardHeader>
                <CardContent>
                    <div className="space-y-4">
                        {filteredViolations.map((violation) => (
                            <div
                                key={violation.id}
                                onClick={() => {
                                    setSelectedViolation(violation);
                                    setSelectedCaseId(null);
                                }}
                                className={cn(
                                    "p-4 rounded-xl border transition-all cursor-pointer hover:shadow-md",
                                    selectedViolation?.id === violation.id
                                        ? "border-blue-300 bg-blue-50/50 ring-1 ring-blue-200"
                                        : "border-slate-200 bg-white hover:border-blue-200"
                                )}
                            >
                                <div className="flex items-start justify-between gap-4">
                                    <div className="flex items-start gap-4">
                                        <div className={cn("p-2 rounded-lg", severityConfig[violation.severity]?.bgLight || 'bg-slate-100')}>
                                            {severityConfig[violation.severity] && React.createElement(severityConfig[violation.severity].icon, {
                                                className: cn("w-5 h-5", severityConfig[violation.severity].iconColor)
                                            })}
                                        </div>
                                        <div>
                                            <div className="flex items-center gap-2 mb-1">
                                                <h3 className="font-semibold text-slate-900">{violation.name}</h3>
                                                <Badge variant="outline" className={cn("capitalize", severityConfig[violation.severity]?.color || 'text-slate-700')}>
                                                    {violation.severity}
                                                </Badge>
                                            </div>
                                            <p className="text-sm text-slate-500 mb-2">{violation.description}</p>
                                            <div className="flex items-center gap-4 text-xs text-slate-500">
                                                <span className="flex items-center gap-1">
                                                    <AlertCircle className="w-3.5 h-3.5" />
                                                    {violation.violationCount} cases
                                                </span>
                                                <span className="flex items-center gap-1">
                                                    <Zap className="w-3.5 h-3.5" />
                                                    {violation.percentOfCases}% of total
                                                </span>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="flex flex-col items-end gap-2">
                                        <Badge className={cn("capitalize", statusConfig[violation.status].color)}>
                                            {statusConfig[violation.status].label}
                                        </Badge>
                                        {violation.status === 'identified' && (
                                            <Button
                                                size="sm"
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    setShowRemediationModal(violation);
                                                }}
                                                className="h-8 bg-gradient-to-r from-blue-600 to-indigo-600"
                                            >
                                                Remediate
                                            </Button>
                                        )}
                                        {violation.status === 'pending_approval' && (
                                            <Button
                                                size="sm"
                                                variant="outline"
                                                onClick={(e) => {
                                                    e.stopPropagation();
                                                    approveRemediation(violation.id);
                                                }}
                                                className="h-8 border-emerald-200 text-emerald-700 hover:bg-emerald-50"
                                            >
                                                <CheckCircle2 className="w-3.5 h-3.5 mr-1" />
                                                Approve
                                            </Button>
                                        )}
                                    </div>
                                </div>

                                {/* Journey Visualization (only if selected) */}
                                <AnimatePresence>
                                    {selectedViolation?.id === violation.id && (
                                        <motion.div
                                            initial={{ height: 0, opacity: 0 }}
                                            animate={{ height: 'auto', opacity: 1 }}
                                            exit={{ height: 0, opacity: 0 }}
                                            className="overflow-hidden"
                                        >
                                            <div className="mt-4 pt-4 border-t border-slate-200">
                                                <div className="flex items-center justify-between mb-4">
                                                    <h4 className="text-sm font-semibold text-slate-900">Violation Journey Example</h4>
                                                    <Select
                                                        value={selectedCaseId || ''}
                                                        onValueChange={(val) => setSelectedCaseId(val)}
                                                    >
                                                        <SelectTrigger className="w-48 h-8 text-xs">
                                                            <SelectValue placeholder="Select Case ID" />
                                                        </SelectTrigger>
                                                        <SelectContent>
                                                            {getAvailableCases().map(c => (
                                                                <SelectItem key={c.caseId} value={c.caseId}>{c.caseId}</SelectItem>
                                                            ))}
                                                        </SelectContent>
                                                    </Select>
                                                </div>
                                                <div className="relative pl-4 border-l-2 border-slate-200 space-y-6">
                                                    {getJourneySteps().map((step, index) => (
                                                        <div key={index} className="relative">
                                                            <div className={cn(
                                                                "absolute -left-[21px] top-1 w-3 h-3 rounded-full border-2",
                                                                step.violation
                                                                    ? "bg-red-500 border-red-200 ring-4 ring-red-50"
                                                                    : step.status === 'completed'
                                                                        ? "bg-emerald-500 border-emerald-200"
                                                                        : "bg-slate-300 border-slate-100"
                                                            )} />
                                                            <div className={cn(
                                                                "p-3 rounded-lg border",
                                                                step.violation
                                                                    ? "bg-red-50 border-red-200"
                                                                    : "bg-slate-50 border-slate-100"
                                                            )}>
                                                                <div className="flex items-center justify-between mb-1">
                                                                    <span className={cn(
                                                                        "font-medium text-sm",
                                                                        step.violation ? "text-red-900" : "text-slate-900"
                                                                    )}>
                                                                        {step.activity}
                                                                    </span>
                                                                    <span className="text-xs text-slate-400 font-mono">{step.timestamp}</span>
                                                                </div>
                                                                <div className="flex items-center gap-4 text-xs text-slate-500">
                                                                    <span className="flex items-center gap-1">
                                                                        <Users className="w-3 h-3" />
                                                                        {step.user}
                                                                    </span>
                                                                    <span className="flex items-center gap-1">
                                                                        <Zap className="w-3 h-3" />
                                                                        {step.system}
                                                                    </span>
                                                                    {step.violation && (
                                                                        <Badge variant="destructive" className="h-5 text-[10px]">Violation Point</Badge>
                                                                    )}
                                                                </div>
                                                            </div>
                                                        </div>
                                                    ))}
                                                </div>
                                            </div>
                                        </motion.div>
                                    )}
                                </AnimatePresence>
                            </div>
                        ))}
                    </div>
                </CardContent>
            </Card>

            {/* Remediation Modal */}
            <AnimatePresence>
                {showRemediationModal && (
                    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
                        <motion.div
                            initial={{ opacity: 0, scale: 0.95 }}
                            animate={{ opacity: 1, scale: 1 }}
                            exit={{ opacity: 0, scale: 0.95 }}
                            className="bg-white rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden"
                        >
                            <div className="p-6 border-b border-slate-100">
                                <div className="flex items-center gap-3 mb-2">
                                    <div className="p-2 rounded-lg bg-blue-100">
                                        <Sparkles className="w-5 h-5 text-blue-600" />
                                    </div>
                                    <h2 className="text-xl font-bold text-slate-900">AI Remediation Assistant</h2>
                                </div>
                                <p className="text-slate-500 text-sm">
                                    Suggested actions to fix violation: <span className="font-medium text-slate-900">{showRemediationModal.name}</span>
                                </p>
                            </div>
                            <div className="p-6 space-y-4">
                                <div className="p-4 rounded-xl bg-slate-50 border border-slate-200">
                                    <h3 className="font-semibold text-slate-900 mb-2">Root Cause Analysis</h3>
                                    <p className="text-sm text-slate-600">
                                        {showRemediationModal.rootCause || "Analysis in progress..."}
                                    </p>
                                </div>
                                <div className="space-y-3">
                                    <h3 className="font-semibold text-slate-900">Recommended Actions</h3>
                                    {showRemediationModal.suggestedActions?.map((action, index) => (
                                        <button
                                            key={index}
                                            onClick={() => handleRemediation(showRemediationModal, action.type === 'automation')}
                                            className="w-full flex items-center gap-3 p-3 rounded-xl border-2 border-slate-100 hover:border-blue-500 hover:bg-blue-50 transition-all group text-left"
                                        >
                                            <div className={cn(
                                                "p-2 rounded-lg transition-colors",
                                                action.type === 'automation' ? "bg-blue-100 group-hover:bg-blue-200" : "bg-slate-100 group-hover:bg-slate-200"
                                            )}>
                                                {action.type === 'automation' ? (
                                                    <Zap className="w-4 h-4 text-blue-600" />
                                                ) : (
                                                    <Users className="w-4 h-4 text-slate-600" />
                                                )}
                                            </div>
                                            <div>
                                                <p className="font-medium text-slate-900">{action.title}</p>
                                                <p className="text-xs text-slate-500">{action.description}</p>
                                            </div>
                                        </button>
                                    ))}
                                    {!showRemediationModal.suggestedActions && (
                                        <p className="text-sm text-slate-500 italic">No automated actions available.</p>
                                    )}
                                </div>
                            </div>
                            <div className="p-4 bg-slate-50 flex justify-end gap-2">
                                <Button variant="ghost" onClick={() => setShowRemediationModal(null)}>Cancel</Button>
                            </div>
                        </motion.div>
                    </div>
                )}
            </AnimatePresence>

            {/* Loading Overlay */}
            <AnimatePresence>
                {remediating && (
                    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-white/80 backdrop-blur-sm">
                        <div className="text-center">
                            <Loader2 className="w-10 h-10 text-blue-600 animate-spin mx-auto mb-4" />
                            <h3 className="text-lg font-semibold text-slate-900">Applying Remediation</h3>
                            <p className="text-slate-500">Updating process configuration...</p>
                        </div>
                    </div>
                )}
            </AnimatePresence>
        </div>
    );
}