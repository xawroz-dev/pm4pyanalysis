// @ts-nocheck
import React from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
    LayoutGrid,
    ChevronRight,
    ChevronDown,
    FolderOpen,
    Folder,
    Plus,
    X,
    GripVertical,
    Wand2,
    CheckCircle2,
    AlertCircle
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

export default function LabelingStep({ initialActivities, aiHierarchy, onChange }) {
    const [hierarchy, setHierarchy] = React.useState(aiHierarchy || []);
    const [unassignedActivities, setUnassignedActivities] = React.useState(initialActivities || []);
    const [draggedItem, setDraggedItem] = React.useState(null);
    const [dragOverItem, setDragOverItem] = React.useState(null);

    React.useEffect(() => {
        if (aiHierarchy) {
            setHierarchy(aiHierarchy);
        }
        if (initialActivities) {
            // Filter out activities that are already in the hierarchy
            const assignedIds = new Set();
            aiHierarchy?.forEach(group => {
                group.children?.forEach(child => assignedIds.add(child.id));
            });
            setUnassignedActivities(initialActivities.filter(a => !assignedIds.has(a.id)));
        }
    }, [initialActivities, aiHierarchy]);

    const handleDragStart = (e, item, sourceGroup = null) => {
        setDraggedItem({ item, sourceGroup });
        e.dataTransfer.effectAllowed = 'move';
    };

    const handleDragOver = (e, targetGroup) => {
        e.preventDefault();
        setDragOverItem(targetGroup);
    };

    const handleDrop = (e, targetGroup) => {
        e.preventDefault();
        if (!draggedItem) return;

        const { item, sourceGroup } = draggedItem;

        // If dropping into the same group, do nothing
        if (sourceGroup?.id === targetGroup.id) {
            setDraggedItem(null);
            setDragOverItem(null);
            return;
        }

        // Remove from source
        if (sourceGroup) {
            setHierarchy(prev => prev.map(g => {
                if (g.id === sourceGroup.id) {
                    return { ...g, children: g.children.filter(c => c.id !== item.id) };
                }
                return g;
            }));
        } else {
            setUnassignedActivities(prev => prev.filter(a => a.id !== item.id));
        }

        // Add to target
        setHierarchy(prev => prev.map(g => {
            if (g.id === targetGroup.id) {
                return { ...g, children: [...(g.children || []), item] };
            }
            return g;
        }));

        setDraggedItem(null);
        setDragOverItem(null);

        // Notify parent of changes
        onChange({ activities: initialActivities, hierarchy });
    };

    const toggleGroup = (groupId) => {
        setHierarchy(prev => prev.map(g => {
            if (g.id === groupId) {
                return { ...g, expanded: !g.expanded };
            }
            return g;
        }));
    };

    return (
        <div className="space-y-6">
            <div className="flex items-center justify-between">
                <div>
                    <h2 className="text-lg font-semibold text-slate-900">Activity Leveling</h2>
                    <p className="text-sm text-slate-500">Organize discovered activities into a business hierarchy</p>
                </div>
                <div className="flex gap-2">
                    <Button variant="outline" size="sm" className="gap-2">
                        <Wand2 className="w-4 h-4" />
                        Auto-Level
                    </Button>
                    <Button variant="outline" size="sm" className="gap-2">
                        <LayoutGrid className="w-4 h-4" />
                        Reset View
                    </Button>
                </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Unassigned Activities */}
                <Card className="lg:col-span-1 border-0 shadow-lg shadow-slate-200/50 h-[600px] flex flex-col">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium flex items-center justify-between">
                            <span>Unassigned Activities</span>
                            <Badge variant="secondary">{unassignedActivities.length}</Badge>
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="flex-1 overflow-y-auto p-2">
                        <div className="space-y-2">
                            {unassignedActivities.map(activity => (
                                <div
                                    key={activity.id}
                                    draggable
                                    onDragStart={(e) => handleDragStart(e, activity)}
                                    className="p-3 bg-white border border-slate-200 rounded-lg shadow-sm cursor-move hover:border-blue-400 hover:shadow-md transition-all group"
                                >
                                    <div className="flex items-center gap-3">
                                        <GripVertical className="w-4 h-4 text-slate-300 group-hover:text-slate-500" />
                                        <span className="text-sm font-medium text-slate-700">{activity.name}</span>
                                    </div>
                                </div>
                            ))}
                            {unassignedActivities.length === 0 && (
                                <div className="text-center py-8 text-slate-400 text-sm">
                                    All activities assigned
                                </div>
                            )}
                        </div>
                    </CardContent>
                </Card>

                {/* Hierarchy Tree */}
                <Card className="lg:col-span-2 border-0 shadow-lg shadow-slate-200/50 h-[600px] flex flex-col">
                    <CardHeader className="pb-2">
                        <CardTitle className="text-sm font-medium flex items-center justify-between">
                            <span>Process Hierarchy</span>
                            <Button variant="ghost" size="sm" className="h-8 text-xs">
                                <Plus className="w-3 h-3 mr-1" />
                                New Group
                            </Button>
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="flex-1 overflow-y-auto p-4">
                        <div className="space-y-4">
                            {hierarchy.map(group => (
                                <div
                                    key={group.id}
                                    onDragOver={(e) => handleDragOver(e, group)}
                                    onDrop={(e) => handleDrop(e, group)}
                                    className={cn(
                                        "border-2 rounded-xl transition-all",
                                        dragOverItem?.id === group.id
                                            ? "border-blue-400 bg-blue-50/50 border-dashed"
                                            : "border-slate-100 bg-slate-50/50"
                                    )}
                                >
                                    <div
                                        className="flex items-center justify-between p-3 cursor-pointer hover:bg-slate-100/50 rounded-t-xl"
                                        onClick={() => toggleGroup(group.id)}
                                    >
                                        <div className="flex items-center gap-3">
                                            {group.expanded ? (
                                                <FolderOpen className="w-5 h-5 text-blue-500" />
                                            ) : (
                                                <Folder className="w-5 h-5 text-slate-400" />
                                            )}
                                            <span className="font-semibold text-slate-900">{group.name}</span>
                                            <Badge variant="outline" className="ml-2 bg-white">
                                                {group.children?.length || 0}
                                            </Badge>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            {group.expanded ? (
                                                <ChevronDown className="w-4 h-4 text-slate-400" />
                                            ) : (
                                                <ChevronRight className="w-4 h-4 text-slate-400" />
                                            )}
                                        </div>
                                    </div>

                                    <AnimatePresence>
                                        {group.expanded && (
                                            <motion.div
                                                initial={{ height: 0, opacity: 0 }}
                                                animate={{ height: 'auto', opacity: 1 }}
                                                exit={{ height: 0, opacity: 0 }}
                                                className="overflow-hidden"
                                            >
                                                <div className="p-3 pt-0 space-y-2">
                                                    {group.children?.map(child => (
                                                        <div
                                                            key={child.id}
                                                            draggable
                                                            onDragStart={(e) => handleDragStart(e, child, group)}
                                                            className="ml-8 p-2 bg-white border border-slate-200 rounded-lg shadow-sm flex items-center justify-between group cursor-move hover:border-blue-400"
                                                        >
                                                            <div className="flex items-center gap-3">
                                                                <div className="w-1.5 h-1.5 rounded-full bg-blue-400" />
                                                                <span className="text-sm text-slate-700">{child.name}</span>
                                                            </div>
                                                            <Button
                                                                variant="ghost"
                                                                size="icon"
                                                                className="h-6 w-6 opacity-0 group-hover:opacity-100"
                                                                onClick={(e) => {
                                                                    e.stopPropagation();
                                                                    // Handle remove
                                                                }}
                                                            >
                                                                <X className="w-3 h-3 text-slate-400 hover:text-red-500" />
                                                            </Button>
                                                        </div>
                                                    ))}
                                                    {(!group.children || group.children.length === 0) && (
                                                        <div className="ml-8 py-4 border-2 border-dashed border-slate-200 rounded-lg flex items-center justify-center text-slate-400 text-sm">
                                                            Drop activities here
                                                        </div>
                                                    )}
                                                </div>
                                            </motion.div>
                                        )}
                                    </AnimatePresence>
                                </div>
                            ))}
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}