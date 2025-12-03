import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue
} from '@/components/ui/select';
import {
    Plus,
    Trash2,
    Database,
    Cloud,
    Upload,
    Radio,
    Server,
    CheckCircle2,
    FileJson,
    GripVertical,
    Clock,
    Hash,
    Activity,
    X,
    ChevronRight,
    Settings
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

const dataSources = [
    { id: 'database', name: 'Database', icon: Database, description: 'Connect to SQL/NoSQL' },
    { id: 'api', name: 'API', icon: Cloud, description: 'REST or GraphQL API' },
    { id: 'file_upload', name: 'File Upload', icon: Upload, description: 'CSV, JSON, Excel' },
    { id: 'streaming', name: 'Streaming', icon: Radio, description: 'Real-time events' },
];

const requiredMappings = [
    { id: 'timestamp', name: 'Timestamp', icon: Clock, description: 'Event time', color: 'from-blue-500 to-blue-600' },
    { id: 'case_id', name: 'Case ID', icon: Hash, description: 'Process instance ID', color: 'from-violet-500 to-violet-600' },
    { id: 'activity', name: 'Activity Name', icon: Activity, description: 'What happened', color: 'from-emerald-500 to-emerald-600' },
];

// Sample fields for demo
const sampleParsedFields = [
    { name: 'event_timestamp', type: 'datetime', sample: '2024-01-15T10:30:00Z' },
    { name: 'request_id', type: 'string', sample: 'REQ-001234' },
    { name: 'activity_code', type: 'string', sample: 'CREDIT_REQ_SUBMIT' },
    { name: 'customer_id', type: 'string', sample: 'CUST-9012' },
    { name: 'credit_amount', type: 'number', sample: '25000.00' },
    { name: 'credit_score', type: 'number', sample: '720' },
    { name: 'risk_level', type: 'string', sample: 'MEDIUM' },
    { name: 'department', type: 'string', sample: 'Credit Operations' },
    { name: 'processor_id', type: 'string', sample: 'EMP-456' },
    { name: 'channel', type: 'string', sample: 'Online Portal' },
    { name: 'status', type: 'string', sample: 'PENDING' },
];

export default function ApplicationOnboarding({ applications, onChange, onConfigureApp }) {
    const [newAppName, setNewAppName] = useState('');
    const [newAppDataSource, setNewAppDataSource] = useState('');
    const [expandedApp, setExpandedApp] = useState(null);
    const [uploading, setUploading] = useState(null);

    const addApplication = () => {
        if (newAppName && newAppDataSource) {
            const newApp = {
                id: Date.now().toString(),
                name: newAppName,
                data_source: newAppDataSource,
                status: 'pending',
                parsed_fields: [],
                field_mappings: {},
                additional_fields: [],
            };
            onChange([...applications, newApp]);
            setNewAppName('');
            setNewAppDataSource('');
            setExpandedApp(newApp.id);
        }
    };

    const removeApplication = (id) => {
        onChange(applications.filter(app => app.id !== id));
        if (expandedApp === id) setExpandedApp(null);
    };

    const handleFileUpload = async (appId, e) => {
        const file = e.target.files?.[0];
        if (!file) return;

        setUploading(appId);

        // Simulate parsing
        await new Promise(resolve => setTimeout(resolve, 1500));

        onChange(applications.map(app =>
            app.id === appId
                ? { ...app, parsed_fields: sampleParsedFields, sample_file_url: URL.createObjectURL(file) }
                : app
        ));
        setUploading(null);
    };

    const handleDragStart = (e, field, appId) => {
        e.dataTransfer.setData('field', JSON.stringify(field));
        e.dataTransfer.setData('appId', appId);
    };

    const handleDrop = (appId, mappingId, e) => {
        e.preventDefault();
        const field = JSON.parse(e.dataTransfer.getData('field'));
        const sourceAppId = e.dataTransfer.getData('appId');

        if (sourceAppId === appId) {
            onChange(applications.map(app =>
                app.id === appId
                    ? { ...app, field_mappings: { ...app.field_mappings, [mappingId]: field.name } }
                    : app
            ));
        }
    };

    const handleAdditionalDrop = (appId, e) => {
        e.preventDefault();
        const field = JSON.parse(e.dataTransfer.getData('field'));
        const sourceAppId = e.dataTransfer.getData('appId');

        if (sourceAppId === appId) {
            onChange(applications.map(app => {
                if (app.id !== appId) return app;
                const existing = app.additional_fields || [];
                if (existing.find(f => f.name === field.name)) return app;
                return { ...app, additional_fields: [...existing, field] };
            }));
        }
    };

    const clearMapping = (appId, mappingId) => {
        onChange(applications.map(app =>
            app.id === appId
                ? { ...app, field_mappings: { ...app.field_mappings, [mappingId]: null } }
                : app
        ));
    };

    const removeAdditionalField = (appId, fieldName) => {
        onChange(applications.map(app =>
            app.id === appId
                ? { ...app, additional_fields: (app.additional_fields || []).filter(f => f.name !== fieldName) }
                : app
        ));
    };

    const isAppConfigured = (app) => {
        return app.field_mappings?.timestamp && app.field_mappings?.case_id && app.field_mappings?.activity;
    };

    return (
        <div className="space-y-6">
            {/* Add new application */}
            <Card className="border-0 shadow-xl shadow-slate-200/50">
                <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-600 to-teal-600 flex items-center justify-center">
                            <Server className="w-5 h-5 text-white" />
                        </div>
                        Add Application
                    </CardTitle>
                    <CardDescription>
                        Add each application separately, then configure field mappings
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4 pt-4">
                    <div className="flex flex-col md:flex-row gap-4">
                        <div className="flex-1">
                            <Label className="mb-2 block">Application Name</Label>
                            <Input
                                placeholder="e.g., Core Banking System, CRM"
                                value={newAppName}
                                onChange={(e) => setNewAppName(e.target.value)}
                                className="h-11"
                            />
                        </div>
                        <div className="w-full md:w-64">
                            <Label className="mb-2 block">Data Source</Label>
                            <Select value={newAppDataSource} onValueChange={setNewAppDataSource}>
                                <SelectTrigger className="h-11">
                                    <SelectValue placeholder="Select source" />
                                </SelectTrigger>
                                <SelectContent>
                                    {dataSources.map((source) => (
                                        <SelectItem key={source.id} value={source.id}>
                                            <div className="flex items-center gap-2">
                                                <source.icon className="w-4 h-4 text-slate-500" />
                                                {source.name}
                                            </div>
                                        </SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>
                        </div>
                        <div className="flex items-end">
                            <Button
                                onClick={addApplication}
                                disabled={!newAppName || !newAppDataSource}
                                className="h-11 bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-700 hover:to-teal-700"
                            >
                                <Plus className="w-4 h-4 mr-2" />
                                Add
                            </Button>
                        </div>
                    </div>
                </CardContent>
            </Card>

            {/* Applications list with expandable config */}
            <AnimatePresence>
                {applications.map((app, index) => {
                    const isExpanded = expandedApp === app.id;
                    const configured = isAppConfigured(app);
                    const hasFields = app.parsed_fields?.length > 0;

                    return (
                        <motion.div
                            key={app.id}
                            initial={{ opacity: 0, y: 20 }}
                            animate={{ opacity: 1, y: 0 }}
                            exit={{ opacity: 0, y: -20 }}
                        >
                            <Card className={cn(
                                "border-0 shadow-xl shadow-slate-200/50 overflow-hidden transition-all",
                                configured && "ring-2 ring-emerald-200"
                            )}>
                                {/* Application Header */}
                                <div
                                    className="flex items-center justify-between p-4 cursor-pointer hover:bg-slate-50"
                                    onClick={() => setExpandedApp(isExpanded ? null : app.id)}
                                >
                                    <div className="flex items-center gap-4">
                                        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white font-bold">
                                            {index + 1}
                                        </div>
                                        <div>
                                            <div className="flex items-center gap-2">
                                                <span className="font-semibold text-slate-900">{app.name}</span>
                                                {configured && (
                                                    <Badge className="bg-emerald-100 text-emerald-700 border-0">
                                                        <CheckCircle2 className="w-3 h-3 mr-1" />
                                                        Configured
                                                    </Badge>
                                                )}
                                                {!configured && hasFields && (
                                                    <Badge className="bg-amber-100 text-amber-700 border-0">
                                                        Mapping Required
                                                    </Badge>
                                                )}
                                            </div>
                                            <p className="text-sm text-slate-500">
                                                {dataSources.find(s => s.id === app.data_source)?.name || app.data_source}
                                            </p>
                                        </div>
                                    </div>
                                    <div className="flex items-center gap-2">
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            onClick={(e) => { e.stopPropagation(); removeApplication(app.id); }}
                                            className="text-slate-400 hover:text-red-500"
                                        >
                                            <Trash2 className="w-4 h-4" />
                                        </Button>
                                        <ChevronRight className={cn(
                                            "w-5 h-5 text-slate-400 transition-transform",
                                            isExpanded && "rotate-90"
                                        )} />
                                    </div>
                                </div>

                                {/* Expanded Configuration */}
                                {isExpanded && (
                                    <div className="border-t border-slate-200 p-6 bg-slate-50/50">
                                        <div className="grid lg:grid-cols-2 gap-6">
                                            {/* Left: File upload and parsed fields */}
                                            <div className="space-y-4">
                                                <div>
                                                    <Label className="mb-2 block font-medium">Sample Log File</Label>
                                                    <label className={cn(
                                                        "flex flex-col items-center justify-center h-32 rounded-xl border-2 border-dashed transition-all cursor-pointer",
                                                        uploading === app.id
                                                            ? "border-amber-400 bg-amber-50"
                                                            : hasFields
                                                                ? "border-emerald-400 bg-emerald-50"
                                                                : "border-slate-300 hover:border-blue-400 hover:bg-blue-50/50"
                                                    )}>
                                                        <input
                                                            type="file"
                                                            accept=".json"
                                                            onChange={(e) => handleFileUpload(app.id, e)}
                                                            className="hidden"
                                                        />
                                                        {uploading === app.id ? (
                                                            <div className="flex flex-col items-center">
                                                                <div className="w-8 h-8 border-2 border-amber-500 border-t-transparent rounded-full animate-spin" />
                                                                <p className="mt-2 text-sm text-amber-600">Parsing file...</p>
                                                            </div>
                                                        ) : hasFields ? (
                                                            <div className="flex flex-col items-center">
                                                                <CheckCircle2 className="w-8 h-8 text-emerald-500" />
                                                                <p className="mt-2 text-sm text-emerald-600 font-medium">{app.parsed_fields.length} fields detected</p>
                                                            </div>
                                                        ) : (
                                                            <div className="flex flex-col items-center">
                                                                <Upload className="w-8 h-8 text-slate-400" />
                                                                <p className="mt-2 text-sm text-slate-600">Upload sample JSON log</p>
                                                            </div>
                                                        )}
                                                    </label>
                                                </div>

                                                {/* Parsed fields */}
                                                {hasFields && (
                                                    <div>
                                                        <Label className="mb-2 block font-medium">Available Fields</Label>
                                                        <div className="max-h-64 overflow-y-auto space-y-1.5 p-3 rounded-xl bg-white border border-slate-200">
                                                            {app.parsed_fields.map((field) => {
                                                                const isMapped = Object.values(app.field_mappings || {}).includes(field.name) ||
                                                                    (app.additional_fields || []).find(f => f.name === field.name);
                                                                return (
                                                                    <div
                                                                        key={field.name}
                                                                        draggable={!isMapped}
                                                                        onDragStart={(e) => handleDragStart(e, field, app.id)}
                                                                        className={cn(
                                                                            "flex items-center gap-3 p-2.5 rounded-lg border transition-all",
                                                                            isMapped
                                                                                ? "bg-slate-50 border-slate-200 opacity-50"
                                                                                : "bg-white border-slate-200 hover:border-blue-300 hover:shadow cursor-grab"
                                                                        )}
                                                                    >
                                                                        <GripVertical className="w-4 h-4 text-slate-400" />
                                                                        <div className="flex-1 min-w-0">
                                                                            <p className="font-medium text-sm text-slate-800 truncate">{field.name}</p>
                                                                            <p className="text-xs text-slate-500 truncate">{field.sample}</p>
                                                                        </div>
                                                                        <Badge variant="outline" className="text-xs">{field.type}</Badge>
                                                                        {isMapped && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                                                                    </div>
                                                                );
                                                            })}
                                                        </div>
                                                    </div>
                                                )}
                                            </div>

                                            {/* Right: Mapping targets */}
                                            <div className="space-y-4">
                                                <div>
                                                    <Label className="mb-2 block font-medium">Required Mappings</Label>
                                                    <div className="space-y-3">
                                                        {requiredMappings.map((mapping) => {
                                                            const Icon = mapping.icon;
                                                            const mappedField = app.field_mappings?.[mapping.id];
                                                            return (
                                                                <div
                                                                    key={mapping.id}
                                                                    onDragOver={(e) => e.preventDefault()}
                                                                    onDrop={(e) => handleDrop(app.id, mapping.id, e)}
                                                                    className={cn(
                                                                        "flex items-center gap-3 p-3 rounded-xl border-2 border-dashed transition-all",
                                                                        mappedField
                                                                            ? "border-emerald-400 bg-emerald-50/50"
                                                                            : "border-slate-200"
                                                                    )}
                                                                >
                                                                    <div className={`w-9 h-9 rounded-lg bg-gradient-to-br ${mapping.color} flex items-center justify-center`}>
                                                                        <Icon className="w-4 h-4 text-white" />
                                                                    </div>
                                                                    <div className="flex-1">
                                                                        <p className="font-medium text-sm text-slate-900">{mapping.name}</p>
                                                                        {mappedField ? (
                                                                            <div className="flex items-center gap-2">
                                                                                <Badge className="bg-emerald-100 text-emerald-700 border-0 text-xs">
                                                                                    {mappedField}
                                                                                </Badge>
                                                                                <button
                                                                                    onClick={() => clearMapping(app.id, mapping.id)}
                                                                                    className="text-slate-400 hover:text-red-500"
                                                                                >
                                                                                    <X className="w-3 h-3" />
                                                                                </button>
                                                                            </div>
                                                                        ) : (
                                                                            <p className="text-xs text-slate-500">{mapping.description}</p>
                                                                        )}
                                                                    </div>
                                                                </div>
                                                            );
                                                        })}
                                                    </div>
                                                </div>

                                                {/* Additional fields */}
                                                <div>
                                                    <Label className="mb-2 block font-medium">Additional Fields (Optional)</Label>
                                                    <div
                                                        onDragOver={(e) => e.preventDefault()}
                                                        onDrop={(e) => handleAdditionalDrop(app.id, e)}
                                                        className={cn(
                                                            "min-h-[80px] p-3 rounded-xl border-2 border-dashed transition-all",
                                                            (app.additional_fields || []).length > 0
                                                                ? "border-blue-300 bg-blue-50/30"
                                                                : "border-slate-200"
                                                        )}
                                                    >
                                                        {(app.additional_fields || []).length > 0 ? (
                                                            <div className="flex flex-wrap gap-2">
                                                                {(app.additional_fields || []).map((field) => (
                                                                    <Badge
                                                                        key={field.name}
                                                                        className="bg-blue-100 text-blue-700 border-0 px-3 py-1.5 flex items-center gap-2"
                                                                    >
                                                                        {field.name}
                                                                        <button
                                                                            onClick={() => removeAdditionalField(app.id, field.name)}
                                                                            className="hover:text-red-600"
                                                                        >
                                                                            <X className="w-3 h-3" />
                                                                        </button>
                                                                    </Badge>
                                                                ))}
                                                            </div>
                                                        ) : (
                                                            <div className="text-center text-slate-500 py-4">
                                                                <Plus className="w-5 h-5 mx-auto mb-1 text-slate-400" />
                                                                <p className="text-sm">Drag additional fields here</p>
                                                            </div>
                                                        )}
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </Card>
                        </motion.div>
                    );
                })}
            </AnimatePresence>

            {applications.length === 0 && (
                <Card className="border-2 border-dashed border-slate-200">
                    <CardContent className="py-12 text-center">
                        <Server className="w-12 h-12 text-slate-300 mx-auto mb-4" />
                        <p className="text-slate-500">No applications added yet</p>
                        <p className="text-sm text-slate-400">Add your first application above to get started</p>
                    </CardContent>
                </Card>
            )}
        </div>
    );
}