import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import {
  Layers,
  Sparkles,
  Plus,
  X,
  GripVertical,
  CheckCircle2,
  Lightbulb
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { motion, AnimatePresence } from 'framer-motion';

// Combined activity colors
const combinedColors = [
  'bg-violet-100 border-violet-300 text-violet-800',
  'bg-emerald-100 border-emerald-300 text-emerald-800',
  'bg-blue-100 border-blue-300 text-blue-800',
  'bg-amber-100 border-amber-300 text-amber-800',
  'bg-rose-100 border-rose-300 text-rose-800',
  'bg-cyan-100 border-cyan-300 text-cyan-800',
];

export default function CombineEventsStep({ combinedGroups, onChange, initialActivities = [], aiSuggestions = [] }) {
  const [activities, setActivities] = useState(initialActivities);
  const [combinedActivities, setCombinedActivities] = useState([]);
  const [suggestions, setSuggestions] = useState(aiSuggestions);
  const [newGroupName, setNewGroupName] = useState('');
  const [draggedActivity, setDraggedActivity] = useState(null);
  const [expandedSuggestion, setExpandedSuggestion] = useState(null);
  const [isDraggingOver, setIsDraggingOver] = useState(false);

  // Update state when props change
  useEffect(() => {
    setActivities(initialActivities);
    setSuggestions(aiSuggestions);
  }, [initialActivities, aiSuggestions]);

  // Helper to get activity names by IDs
  const getActivityNames = (ids) => {
    return initialActivities.filter(a => ids.includes(a.id)).map(a => a.name);
  };

  // Drag and Drop Handlers
  const handleDragStart = (e, activity) => {
    setDraggedActivity(activity);
    e.dataTransfer.setData('activityId', activity.id);
    e.dataTransfer.effectAllowed = 'move';
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDraggingOver(true);
  };

  const handleDragLeave = () => {
    setIsDraggingOver(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDraggingOver(false);

    if (!draggedActivity) return;

    // If dropping into "Create New Group" zone
    if (newGroupName.trim()) {
      createNewGroupWithActivity(draggedActivity);
    }
  };

  const createNewGroupWithActivity = (activity) => {
    const colorIndex = combinedActivities.length % combinedColors.length;
    const newCombined = {
      id: Date.now(),
      name: newGroupName,
      originalActivities: [activity],
      colorClass: combinedColors[colorIndex],
    };
    setCombinedActivities([...combinedActivities, newCombined]);
    setActivities(prev => prev.filter(a => a.id !== activity.id));
    setNewGroupName('');
    setDraggedActivity(null);
  };

  const handleDropOnExistingGroup = (e, group) => {
    e.preventDefault();
    if (!draggedActivity) return;

    const updatedGroup = {
      ...group,
      originalActivities: [...group.originalActivities, draggedActivity].sort((a, b) => a.id - b.id)
    };

    setCombinedActivities(prev => prev.map(g => g.id === group.id ? updatedGroup : g));
    setActivities(prev => prev.filter(a => a.id !== draggedActivity.id));
    setDraggedActivity(null);
  };

  const applyAISuggestion = (suggestion) => {
    const colorIndex = combinedActivities.length % combinedColors.length;
    const newCombined = {
      id: Date.now(),
      name: suggestion.name,
      originalActivities: activities.filter(a => suggestion.activityIds.includes(a.id)),
      colorClass: combinedColors[colorIndex],
    };
    setCombinedActivities([...combinedActivities, newCombined]);
    setActivities(prev => prev.filter(a => !suggestion.activityIds.includes(a.id)));

    // Remove from suggestions list
    setSuggestions(prev => prev.filter(s => s.name !== suggestion.name));
  };

  const applyAllSuggestions = () => {
    const newCombined = suggestions.map((suggestion, index) => ({
      id: Date.now() + index,
      name: suggestion.name,
      originalActivities: initialActivities.filter(a => suggestion.activityIds.includes(a.id)),
      colorClass: combinedColors[index % combinedColors.length],
    }));
    setCombinedActivities([...combinedActivities, ...newCombined]);
    const allSuggestedIds = suggestions.flatMap(s => s.activityIds);
    setActivities(prev => prev.filter(a => !allSuggestedIds.includes(a.id)));
    setSuggestions([]);
  };

  const uncombine = (combinedId) => {
    const combined = combinedActivities.find(c => c.id === combinedId);
    if (combined) {
      setActivities(prev => [...prev, ...combined.originalActivities].sort((a, b) => a.id - b.id));
      setCombinedActivities(prev => prev.filter(c => c.id !== combinedId));
    }
  };

  return (
    <div className="space-y-6">
      {/* AI Suggestions */}
      {suggestions.length > 0 && (
        <Card className="border-2 border-amber-200 shadow-xl shadow-amber-100/50 bg-gradient-to-br from-amber-50 to-orange-50">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-3 text-amber-900">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-amber-500 to-orange-600 flex items-center justify-center">
                <Sparkles className="w-5 h-5 text-white" />
              </div>
              AI Suggested Combinations
            </CardTitle>
            <CardDescription className="text-amber-800">
              Based on activity patterns, AI suggests grouping these related events
            </CardDescription>
          </CardHeader>
          <CardContent className="pt-4">
            <div className="space-y-3 mb-4">
              {suggestions.map((suggestion, index) => {
                const isExpanded = expandedSuggestion === index;
                const activityNames = getActivityNames(suggestion.activityIds);
                return (
                  <div
                    key={index}
                    className="rounded-xl bg-white border border-amber-200 overflow-hidden"
                  >
                    <div
                      className="flex items-center justify-between p-4 cursor-pointer hover:bg-amber-50"
                      onClick={() => setExpandedSuggestion(isExpanded ? null : index)}
                    >
                      <div className="flex items-center gap-4">
                        <div className="w-10 h-10 rounded-lg bg-amber-100 flex items-center justify-center">
                          <Layers className="w-5 h-5 text-amber-600" />
                        </div>
                        <div>
                          <p className="font-medium text-slate-900">{suggestion.name}</p>
                          <p className="text-sm text-slate-500">
                            {suggestion.activityIds.length} activities • {suggestion.confidence}% confidence
                          </p>
                        </div>
                      </div>
                      <Button
                        size="sm"
                        onClick={(e) => { e.stopPropagation(); applyAISuggestion(suggestion); }}
                        className="bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-600 hover:to-orange-600"
                      >
                        <CheckCircle2 className="w-4 h-4 mr-2" />
                        Combine
                      </Button>
                    </div>
                    {isExpanded && (
                      <div className="px-4 pb-4 border-t border-amber-100 pt-3">
                        <p className="text-xs font-medium text-slate-500 mb-2">Activities to combine:</p>
                        <div className="flex flex-wrap gap-2">
                          {activityNames.map((name, i) => (
                            <Badge key={i} variant="outline" className="text-xs bg-slate-50">
                              {name}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
            <Button onClick={applyAllSuggestions} className="w-full" variant="outline">
              <Sparkles className="w-4 h-4 mr-2" />
              Apply All AI Suggestions
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Manual Creation Zone */}
      <Card className="border-dashed border-2 border-slate-300 bg-slate-50/50">
        <CardContent className="pt-6">
          <div className="flex items-center gap-4">
            <div className="flex-1">
              <Input
                placeholder="Enter new group name (e.g., 'Document Review')"
                value={newGroupName}
                onChange={(e) => setNewGroupName(e.target.value)}
                className="bg-white"
              />
            </div>
            <div
              className={cn(
                "flex-1 h-10 rounded-md border-2 border-dashed flex items-center justify-center transition-colors",
                isDraggingOver ? "border-blue-500 bg-blue-50 text-blue-600" : "border-slate-300 text-slate-400",
                newGroupName ? "opacity-100" : "opacity-50 cursor-not-allowed"
              )}
              onDragOver={newGroupName ? handleDragOver : undefined}
              onDragLeave={handleDragLeave}
              onDrop={handleDrop}
            >
              {isDraggingOver ? "Drop activity here to create group" : "Drag activity here to start group"}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Activities List */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Uncombined Activities */}
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold text-slate-900 flex items-center gap-2">
              <Layers className="w-4 h-4 text-slate-500" />
              Uncombined Activities
            </h3>
            <Badge variant="secondary" className="bg-slate-100 text-slate-600">
              {activities.length}
            </Badge>
          </div>
          <div className="space-y-2 min-h-[200px]">
            {activities.map((activity) => (
              <div
                key={activity.id}
                draggable
                onDragStart={(e) => handleDragStart(e, activity)}
                className="p-3 bg-white border border-slate-200 rounded-lg shadow-sm cursor-grab active:cursor-grabbing hover:border-blue-400 hover:shadow-md transition-all group"
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-slate-700">{activity.name}</span>
                  <GripVertical className="w-4 h-4 text-slate-300 group-hover:text-slate-400" />
                </div>
              </div>
            ))}
            {activities.length === 0 && (
              <div className="h-full flex flex-col items-center justify-center text-slate-400 border-2 border-dashed border-slate-200 rounded-lg p-8">
                <CheckCircle2 className="w-8 h-8 mb-2 text-emerald-500" />
                <p className="text-sm">All activities combined!</p>
              </div>
            )}
          </div>
        </div>

        {/* Combined Groups */}
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="font-semibold text-slate-900 flex items-center gap-2">
              <Sparkles className="w-4 h-4 text-slate-500" />
              Combined Groups
            </h3>
            <Badge variant="secondary" className="bg-slate-100 text-slate-600">
              {combinedActivities.length}
            </Badge>
          </div>
          <div className="space-y-4 min-h-[200px]">
            {combinedActivities.map((group) => (
              <div
                key={group.id}
                onDragOver={(e) => e.preventDefault()}
                onDrop={(e) => handleDropOnExistingGroup(e, group)}
                className={cn(
                  "p-4 rounded-xl border-2 transition-all",
                  group.colorClass
                )}
              >
                <div className="flex items-center justify-between mb-3">
                  <span className="font-semibold">{group.name}</span>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => uncombine(group.id)}
                    className="h-6 w-6 p-0 hover:bg-white/20"
                  >
                    <X className="w-4 h-4" />
                  </Button>
                </div>
                <div className="space-y-2">
                  {group.originalActivities.map((activity) => (
                    <div
                      key={activity.id}
                      className="text-xs bg-white/50 p-2 rounded border border-black/5"
                    >
                      {activity.name}
                    </div>
                  ))}
                </div>
              </div>
            ))}
            {combinedActivities.length === 0 && (
              <div className="h-full flex flex-col items-center justify-center text-slate-400 border-2 border-dashed border-slate-200 rounded-lg p-8">
                <Lightbulb className="w-8 h-8 mb-2" />
                <p className="text-sm text-center">Drag activities here or use AI suggestions</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}