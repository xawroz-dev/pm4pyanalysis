import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
    Sparkles,
    Check,
    X,
    Pencil,
    ArrowRight,
    Lightbulb,
    Loader2
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

export default function AINamingStep({ initialActivities = [], onUpdate }) {
    const [localActivities, setLocalActivities] = useState([]);
    const [analyzing, setAnalyzing] = useState(true);
    const [editingId, setEditingId] = useState(null);
    const [editValue, setEditValue] = useState('');

    useEffect(() => {
        // Simulate AI analysis
        const timer = setTimeout(() => {
            setLocalActivities(initialActivities.map((a, i) => ({
                ...a,
                id: i,
                displayName: a.original,
                accepted: false,
            })));
            setAnalyzing(false);
        }, 2000);

        return () => clearTimeout(timer);
    }, [initialActivities]);

    const acceptSuggestion = (id) => {
        setLocalActivities(prev => prev.map(a =>
            a.id === id ? { ...a, displayName: a.aiSuggestion, accepted: true } : a
        ));
    };

    const acceptAll = () => {
        setLocalActivities(prev => prev.map(a => ({
            ...a,
            displayName: a.aiSuggestion,
            accepted: true,
        })));
    };

    const startEdit = (activity) => {
        setEditingId(activity.id);
        setEditValue(activity.displayName);
    };

    const saveEdit = (id) => {
        setLocalActivities(prev => prev.map(a =>
            a.id === id ? { ...a, displayName: editValue, accepted: true } : a
        ));
        setEditingId(null);
        setEditValue('');
    };

    const acceptedCount = localActivities.filter(a => a.accepted).length;

    return (
        <div className="space-y-6">
            <Card className="border-0 shadow-xl shadow-slate-200/50">
                <CardHeader className="pb-2">
                    <CardTitle className="flex items-center gap-3">
                        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-amber-500 to-orange-600 flex items-center justify-center">
                            <Sparkles className="w-5 h-5 text-white" />
                        </div>
                        AI-Assisted Activity Naming
                    </CardTitle>
                    <CardDescription>
                        Our AI analyzes cryptic activity codes and suggests human-readable names
                    </CardDescription>
                </CardHeader>
                <CardContent className="pt-6">
                    {analyzing ? (
                        <div className="py-16 text-center">
                            <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-amber-100 to-orange-100 mb-4">
                                <Loader2 className="w-8 h-8 text-amber-600 animate-spin" />
                            </div>
                            <h3 className="text-lg font-semibold text-slate-900">Analyzing Activities</h3>
                            <p className="text-slate-500 mt-2">AI is reviewing activity names and generating suggestions...</p>
                            <div className="flex items-center justify-center gap-1 mt-6">
                                {[0, 1, 2, 3, 4].map((i) => (
                                    <div
                                        key={i}
                                        className="w-2 h-2 rounded-full bg-amber-500 animate-pulse"
                                        style={{ animationDelay: `${i * 0.15}s` }}
                                    />
                                ))}
                            </div>
                        </div>
                    ) : (
                        <>
                            {/* Stats header */}
                            <div className="flex items-center justify-between mb-6 p-4 rounded-xl bg-gradient-to-r from-amber-50 to-orange-50 border border-amber-200">
                                <div className="flex items-center gap-3">
                                    <Lightbulb className="w-5 h-5 text-amber-600" />
                                    <div>
                                        <p className="text-sm font-medium text-amber-900">
                                            {localActivities.length} activities analyzed
                                        </p>
                                        <p className="text-xs text-amber-700">
                                            {acceptedCount} of {localActivities.length} renamed
                                        </p>
                                    </div>
                                </div>
                                <Button
                                    onClick={acceptAll}
                                    className="bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-600 hover:to-orange-600"
                                >
                                    <Check className="w-4 h-4 mr-2" />
                                    Accept All Suggestions
                                </Button>
                            </div>

                            {/* Activity list */}
                            <div className="space-y-3">
                                <AnimatePresence>
                                    {localActivities.map((activity) => (
                                        <motion.div
                                            key={activity.id}
                                            initial={{ opacity: 0, y: 10 }}
                                            animate={{ opacity: 1, y: 0 }}
                                            className={cn(
                                                "p-4 rounded-xl border-2 transition-all",
                                                activity.accepted
                                                    ? "border-emerald-200 bg-emerald-50/50"
                                                    : "border-slate-200 bg-white hover:border-amber-200"
                                            )}
                                        >
                                            <div className="flex items-start gap-4">
                                                {/* Original name */}
                                                <div className="flex-1">
                                                    <div className="flex items-center gap-2 mb-2">
                                                        <Badge variant="outline" className="font-mono text-xs bg-slate-100">
                                                            {activity.original}
                                                        </Badge>
                                                        <ArrowRight className="w-4 h-4 text-slate-400" />
                                                    </div>

                                                    {editingId === activity.id ? (
                                                        <div className="flex items-center gap-2">
                                                            <Input
                                                                value={editValue}
                                                                onChange={(e) => setEditValue(e.target.value)}
                                                                className="flex-1"
                                                                autoFocus
                                                            />
                                                            <Button size="sm" onClick={() => saveEdit(activity.id)}>
                                                                <Check className="w-4 h-4" />
                                                            </Button>
                                                            <Button size="sm" variant="ghost" onClick={() => setEditingId(null)}>
                                                                <X className="w-4 h-4" />
                                                            </Button>
                                                        </div>
                                                    ) : (
                                                        <div className="flex items-center gap-3">
                                                            <span className={cn(
                                                                "font-medium",
                                                                activity.accepted ? "text-emerald-700" : "text-slate-900"
                                                            )}>
                                                                {activity.displayName}
                                                            </span>
                                                            {activity.accepted && (
                                                                <Badge className="bg-emerald-100 text-emerald-700 border-0">
                                                                    <Check className="w-3 h-3 mr-1" />
                                                                    Renamed
                                                                </Badge>
                                                            )}
                                                        </div>
                                                    )}
                                                </div>

                                                {/* Actions */}
                                                {!activity.accepted && editingId !== activity.id && (
                                                    <div className="flex items-center gap-2">
                                                        {/* AI suggestion popup */}
                                                        <div className="relative group">
                                                            <Button
                                                                size="sm"
                                                                onClick={() => acceptSuggestion(activity.id)}
                                                                className="bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-600 hover:to-orange-600 gap-2"
                                                            >
                                                                <Sparkles className="w-3.5 h-3.5" />
                                                                Use AI Suggestion
                                                            </Button>
                                                            <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 px-3 py-2 bg-slate-900 text-white text-xs rounded-lg opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap pointer-events-none z-10">
                                                                "{activity.aiSuggestion}"
                                                                <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-slate-900" />
                                                            </div>
                                                        </div>
                                                        <Button
                                                            size="sm"
                                                            variant="outline"
                                                            onClick={() => startEdit(activity)}
                                                        >
                                                            <Pencil className="w-3.5 h-3.5" />
                                                        </Button>
                                                    </div>
                                                )}

                                                {activity.accepted && editingId !== activity.id && (
                                                    <Button
                                                        size="sm"
                                                        variant="ghost"
                                                        onClick={() => startEdit(activity)}
                                                        className="text-slate-400 hover:text-slate-600"
                                                    >
                                                        <Pencil className="w-4 h-4" />
                                                    </Button>
                                                )}
                                            </div>

                                            {/* Category badge */}
                                            <div className="mt-3">
                                                <Badge variant="outline" className="text-xs">
                                                    {activity.category}
                                                </Badge>
                                            </div>
                                        </motion.div>
                                    ))}
                                </AnimatePresence>
                            </div>
                        </>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}