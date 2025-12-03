import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue
} from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
    Link2,
    ArrowRight,
    CheckCircle2,
    Server,
    Sparkles,
    Zap
} from 'lucide-react';
import { cn } from '@/lib/utils';

const availableKeys = [
    'transaction_id',
    'customer_id',
    'order_id',
    'session_id',
    'user_id',
    'correlation_id',
];

export default function StitchingStep({ applications, stitchingConfig, onChange, sampleJourney = [] }) {
    const [selectedApps, setSelectedApps] = useState(applications.map(a => a.id));
    const [keyMappings, setKeyMappings] = useState(stitchingConfig || {});
    const [showVisualization, setShowVisualization] = useState(false);

    // Initialize defaults from case_id
    useEffect(() => {
        const newMappings = { ...keyMappings };
        let changed = false;
        applications.forEach(app => {
            if (!newMappings[app.id] && app.field_mappings?.case_id) {
                newMappings[app.id] = app.field_mappings.case_id;
                changed = true;
            }
        });
        if (changed) {
            setKeyMappings(newMappings);
            onChange(newMappings);
        }
    }, [applications]);

    const handleKeyChange = (appId, key) => {
        const newMappings = { ...keyMappings, [appId]: key };
        setKeyMappings(newMappings);
        onChange(newMappings);
    };

    const allMapped = selectedApps.every(appId => keyMappings[appId]);

    return (
        <div className="space-y-6">
            <Card className="border-0 shadow-xl shadow-slate-200/50">
                <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-600 to-purple-600 flex items-center justify-center">
                            <Link2 className="w-5 h-5 text-white" />
                        </div>
                        Journey Stitching
                    </CardTitle>
                    <CardDescription>
                        Connect journeys across applications by mapping common identifiers
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6 pt-6">
                    {/* Application key mapping */}
                    <div className="space-y-4">
                        <p className="text-sm font-medium text-slate-700">
                            Select the key field from each application that can be used to link events together
                        </p>

                        <div className="grid gap-4">
                            {applications.map((app, index) => (
                                <div
                                    key={app.id}
                                    className="flex items-center gap-4 p-4 rounded-xl bg-gradient-to-r from-slate-50 to-white border border-slate-200"
                                >
                                    <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white font-semibold">
                                        {index + 1}
                                    </div>
                                    <div className="flex-1">
                                        <p className="font-medium text-slate-900">{app.name}</p>
                                    </div>
                                    <div className="flex items-center gap-3">
                                        <ArrowRight className="w-4 h-4 text-slate-400" />
                                        <span className="text-sm font-medium text-slate-600 whitespace-nowrap">Correlation Field:</span>
                                        <Select
                                            value={keyMappings[app.id] || ''}
                                            onValueChange={(value) => handleKeyChange(app.id, value)}
                                        >
                                            <SelectTrigger className="w-48">
                                                <SelectValue placeholder="Select stitching key" />
                                            </SelectTrigger>
                                            <SelectContent>
                                                {(app.parsed_fields && app.parsed_fields.length > 0) ? (
                                                    app.parsed_fields.map((field) => (
                                                        <SelectItem key={field.name} value={field.name}>{field.name}</SelectItem>
                                                    ))
                                                ) : (
                                                    availableKeys.map((key) => (
                                                        <SelectItem key={key} value={key}>{key}</SelectItem>
                                                    ))
                                                )}
                                            </SelectContent>
                                        </Select>
                                        {keyMappings[app.id] && (
                                            <CheckCircle2 className="w-5 h-5 text-emerald-500" />
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Connection visualization */}
                    {allMapped && (
                        <div className="p-6 rounded-2xl bg-gradient-to-br from-violet-50 to-purple-50 border border-violet-200">
                            <div className="flex items-center gap-2 mb-4">
                                <Zap className="w-5 h-5 text-violet-600" />
                                <p className="font-medium text-violet-900">Stitching Configuration</p>
                            </div>
                            <div className="flex items-center justify-center gap-4 flex-wrap">
                                {applications.map((app, index) => (
                                    <React.Fragment key={app.id}>
                                        <div className="flex flex-col items-center gap-2">
                                            <div className="w-14 h-14 rounded-2xl bg-white shadow-lg flex items-center justify-center">
                                                <Server className="w-6 h-6 text-slate-600" />
                                            </div>
                                            <span className="text-sm font-medium text-slate-800">{app.name}</span>
                                            <Badge className="bg-violet-100 text-violet-700 border-0 text-xs">
                                                {keyMappings[app.id]}
                                            </Badge>
                                        </div>
                                        {index < applications.length - 1 && (
                                            <div className="flex items-center">
                                                <div className="w-12 h-0.5 bg-gradient-to-r from-violet-400 to-purple-400" />
                                                <Link2 className="w-5 h-5 text-violet-500 -mx-1" />
                                                <div className="w-12 h-0.5 bg-gradient-to-r from-purple-400 to-violet-400" />
                                            </div>
                                        )}
                                    </React.Fragment>
                                ))}
                            </div>
                        </div>
                    )}
                </CardContent>
            </Card>

            {/* Journey Preview */}
            {allMapped && sampleJourney.length > 0 && (
                <Card className="border-0 shadow-xl shadow-slate-200/50 overflow-hidden">
                    <CardHeader className="bg-gradient-to-r from-emerald-500 to-teal-500 text-white">
                        <CardTitle className="flex items-center gap-3">
                            <Sparkles className="w-5 h-5" />
                            Preview: Stitched Journey
                        </CardTitle>
                        <CardDescription className="text-emerald-100">
                            This is how a unified journey will look across your applications
                        </CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                        <div className="relative">
                            {/* Timeline line */}
                            <div className="absolute left-6 top-0 bottom-0 w-0.5 bg-gradient-to-b from-emerald-400 via-teal-400 to-blue-400" />

                            {/* Journey events */}
                            <div className="space-y-4">
                                {sampleJourney.map((event, index) => (
                                    <div key={index} className="flex items-start gap-4 relative">
                                        <div className={cn(
                                            "w-12 h-12 rounded-full flex items-center justify-center z-10",
                                            index === 0 ? "bg-gradient-to-br from-emerald-500 to-emerald-600" :
                                                index === sampleJourney.length - 1 ? "bg-gradient-to-br from-blue-500 to-blue-600" :
                                                    "bg-white border-2 border-teal-400"
                                        )}>
                                            <span className={cn(
                                                "text-sm font-bold",
                                                index === 0 || index === sampleJourney.length - 1 ? "text-white" : "text-teal-600"
                                            )}>
                                                {index + 1}
                                            </span>
                                        </div>
                                        <div className="flex-1 pt-2">
                                            <div className="flex items-center gap-3">
                                                <p className="font-medium text-slate-900">{event.activity}</p>
                                                <Badge variant="outline" className="text-xs">
                                                    {event.app}
                                                </Badge>
                                            </div>
                                            <p className="text-sm text-slate-500 mt-1">{event.time}</p>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </CardContent>
                </Card>
            )}
        </div>
    );
}