import React, { useState, useCallback } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from '@/components/ui/select';
import {
  Upload,
  FileJson,
  Clock,
  Hash,
  Activity,
  GripVertical,
  CheckCircle2,
  Plus,
  X,
  AlertCircle,
  Sparkles
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { base44 } from '@/api/base44Client';

// Sample parsed fields for demo
const sampleFields = [
  { name: 'event_timestamp', type: 'datetime', sample: '2024-01-15T10:30:00Z' },
  { name: 'transaction_id', type: 'string', sample: 'TXN-001234' },
  { name: 'activity_name', type: 'string', sample: 'Order Created' },
  { name: 'user_id', type: 'string', sample: 'USR-5678' },
  { name: 'customer_id', type: 'string', sample: 'CUST-9012' },
  { name: 'order_amount', type: 'number', sample: '1250.00' },
  { name: 'currency', type: 'string', sample: 'USD' },
  { name: 'status', type: 'string', sample: 'COMPLETED' },
  { name: 'department', type: 'string', sample: 'Sales' },
  { name: 'region', type: 'string', sample: 'North America' },
  { name: 'channel', type: 'string', sample: 'Web' },
  { name: 'priority', type: 'string', sample: 'HIGH' },
];

const requiredMappings = [
  { id: 'timestamp', name: 'Timestamp', icon: Clock, description: 'When did this event occur?', color: 'from-blue-500 to-blue-600' },
  { id: 'case_id', name: 'Case ID', icon: Hash, description: 'Unique identifier for each process instance', color: 'from-violet-500 to-violet-600' },
  { id: 'activity', name: 'Activity Name', icon: Activity, description: 'What happened in this step?', color: 'from-emerald-500 to-emerald-600' },
];

export default function FieldMappingStep({ applications, mappings, onChange }) {
  const [selectedApp, setSelectedApp] = useState(applications[0]?.id || '');
  const [parsedFields, setParsedFields] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [draggedField, setDraggedField] = useState(null);
  const [currentMappings, setCurrentMappings] = useState(mappings || {});
  const [additionalFields, setAdditionalFields] = useState([]);

  const handleFileUpload = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setUploading(true);

    // Simulate parsing
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Use sample fields for demo
    setParsedFields(sampleFields);
    setUploading(false);
  };

  const handleDragStart = (field) => {
    setDraggedField(field);
  };

  const handleDrop = (mappingId) => {
    if (draggedField) {
      const newMappings = { ...currentMappings, [selectedApp]: { ...currentMappings[selectedApp], [mappingId]: draggedField.name } };
      setCurrentMappings(newMappings);
      onChange(newMappings);
      setDraggedField(null);
    }
  };

  const handleAdditionalDrop = () => {
    if (draggedField && !additionalFields.find(f => f.name === draggedField.name)) {
      setAdditionalFields([...additionalFields, draggedField]);
      const newMappings = {
        ...currentMappings,
        [selectedApp]: {
          ...currentMappings[selectedApp],
          additional: [...(currentMappings[selectedApp]?.additional || []), draggedField.name]
        }
      };
      setCurrentMappings(newMappings);
      onChange(newMappings);
      setDraggedField(null);
    }
  };

  const removeAdditionalField = (fieldName) => {
    setAdditionalFields(additionalFields.filter(f => f.name !== fieldName));
  };

  const clearMapping = (mappingId) => {
    const newMappings = { ...currentMappings, [selectedApp]: { ...currentMappings[selectedApp], [mappingId]: null } };
    setCurrentMappings(newMappings);
    onChange(newMappings);
  };

  const currentAppMappings = currentMappings[selectedApp] || {};

  return (
    <div className="space-y-6">
      {/* Application selector */}
      {applications.length > 1 && (
        <Card className="border-0 shadow-lg shadow-slate-200/50">
          <CardContent className="p-4">
            <div className="flex items-center gap-4">
              <span className="text-sm font-medium text-slate-700">Select Application:</span>
              <Select value={selectedApp} onValueChange={setSelectedApp}>
                <SelectTrigger className="w-64">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {applications.map((app) => (
                    <SelectItem key={app.id} value={app.id}>{app.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </CardContent>
        </Card>
      )}

      <div className="grid lg:grid-cols-2 gap-6">
        {/* File upload and parsed fields */}
        <Card className="border-0 shadow-xl shadow-slate-200/50">
          <CardHeader>
            <CardTitle className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-amber-500 to-orange-600 flex items-center justify-center">
                <FileJson className="w-5 h-5 text-white" />
              </div>
              Upload Sample Log
            </CardTitle>
            <CardDescription>
              Upload a JSON log file to automatically detect fields
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            {/* Upload area */}
            <label className={cn(
              "flex flex-col items-center justify-center h-32 rounded-xl border-2 border-dashed transition-all cursor-pointer",
              uploading
                ? "border-amber-400 bg-amber-50"
                : parsedFields.length > 0
                  ? "border-emerald-400 bg-emerald-50"
                  : "border-slate-300 hover:border-blue-400 hover:bg-blue-50/50"
            )}>
              <input
                type="file"
                accept=".json"
                onChange={handleFileUpload}
                className="hidden"
              />
              {uploading ? (
                <div className="flex flex-col items-center">
                  <div className="w-8 h-8 border-2 border-amber-500 border-t-transparent rounded-full animate-spin" />
                  <p className="mt-2 text-sm text-amber-600">Parsing file...</p>
                </div>
              ) : parsedFields.length > 0 ? (
                <div className="flex flex-col items-center">
                  <CheckCircle2 className="w-8 h-8 text-emerald-500" />
                  <p className="mt-2 text-sm text-emerald-600 font-medium">{parsedFields.length} fields detected</p>
                  <p className="text-xs text-slate-500">Click to upload different file</p>
                </div>
              ) : (
                <div className="flex flex-col items-center">
                  <Upload className="w-8 h-8 text-slate-400" />
                  <p className="mt-2 text-sm text-slate-600">Drop your JSON file here</p>
                  <p className="text-xs text-slate-400">or click to browse</p>
                </div>
              )}
            </label>

            {/* Parsed fields list */}
            {parsedFields.length > 0 && (
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <p className="text-sm font-medium text-slate-700">Available Fields</p>
                  <Badge variant="outline" className="text-xs">Drag to map</Badge>
                </div>
                <div className="max-h-80 overflow-y-auto space-y-1.5 pr-2">
                  {parsedFields.map((field) => {
                    const isMapped = Object.values(currentAppMappings).includes(field.name) ||
                      additionalFields.find(f => f.name === field.name);
                    return (
                      <div
                        key={field.name}
                        draggable={!isMapped}
                        onDragStart={() => handleDragStart(field)}
                        className={cn(
                          "flex items-center gap-3 p-3 rounded-lg border transition-all",
                          isMapped
                            ? "bg-slate-50 border-slate-200 opacity-50 cursor-not-allowed"
                            : "bg-white border-slate-200 hover:border-blue-300 hover:shadow-md cursor-grab active:cursor-grabbing"
                        )}
                      >
                        <GripVertical className="w-4 h-4 text-slate-400" />
                        <div className="flex-1 min-w-0">
                          <p className="font-medium text-sm text-slate-800 truncate">{field.name}</p>
                          <p className="text-xs text-slate-500 truncate">Sample: {field.sample}</p>
                        </div>
                        <Badge variant="outline" className="text-xs flex-shrink-0">
                          {field.type}
                        </Badge>
                        {isMapped && <CheckCircle2 className="w-4 h-4 text-emerald-500" />}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Mapping targets */}
        <div className="space-y-4">
          {/* Required mappings */}
          <Card className="border-0 shadow-xl shadow-slate-200/50">
            <CardHeader className="pb-4">
              <CardTitle className="flex items-center gap-2 text-lg">
                <AlertCircle className="w-5 h-5 text-amber-500" />
                Required Mappings
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {requiredMappings.map((mapping) => {
                const Icon = mapping.icon;
                const mappedField = currentAppMappings[mapping.id];
                return (
                  <div
                    key={mapping.id}
                    onDragOver={(e) => e.preventDefault()}
                    onDrop={() => handleDrop(mapping.id)}
                    className={cn(
                      "flex items-center gap-4 p-4 rounded-xl border-2 border-dashed transition-all",
                      mappedField
                        ? "border-emerald-400 bg-emerald-50/50"
                        : draggedField
                          ? "border-blue-400 bg-blue-50"
                          : "border-slate-200"
                    )}
                  >
                    <div className={`w-10 h-10 rounded-xl bg-gradient-to-br ${mapping.color} flex items-center justify-center`}>
                      <Icon className="w-5 h-5 text-white" />
                    </div>
                    <div className="flex-1">
                      <p className="font-medium text-slate-900">{mapping.name}</p>
                      {mappedField ? (
                        <div className="flex items-center gap-2 mt-1">
                          <Badge className="bg-emerald-100 text-emerald-700 border-0">
                            {mappedField}
                          </Badge>
                          <button
                            onClick={() => clearMapping(mapping.id)}
                            className="text-slate-400 hover:text-red-500"
                          >
                            <X className="w-3.5 h-3.5" />
                          </button>
                        </div>
                      ) : (
                        <p className="text-xs text-slate-500">{mapping.description}</p>
                      )}
                    </div>
                    {mappedField && <CheckCircle2 className="w-5 h-5 text-emerald-500" />}
                  </div>
                );
              })}
            </CardContent>
          </Card>

          {/* Additional fields */}
          <Card className="border-0 shadow-xl shadow-slate-200/50">
            <CardHeader className="pb-4">
              <CardTitle className="flex items-center gap-2 text-lg">
                <Plus className="w-5 h-5 text-blue-500" />
                Additional Fields (Optional)
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div
                onDragOver={(e) => e.preventDefault()}
                onDrop={handleAdditionalDrop}
                className={cn(
                  "min-h-[100px] p-4 rounded-xl border-2 border-dashed transition-all",
                  additionalFields.length > 0
                    ? "border-blue-300 bg-blue-50/30"
                    : draggedField
                      ? "border-blue-400 bg-blue-50"
                      : "border-slate-200"
                )}
              >
                {additionalFields.length > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {additionalFields.map((field) => (
                      <Badge
                        key={field.name}
                        className="bg-blue-100 text-blue-700 border-0 px-3 py-1.5 text-sm flex items-center gap-2"
                      >
                        {field.name}
                        <button
                          onClick={() => removeAdditionalField(field.name)}
                          className="hover:text-red-600"
                        >
                          <X className="w-3 h-3" />
                        </button>
                      </Badge>
                    ))}
                  </div>
                ) : (
                  <div className="text-center text-slate-500">
                    <Plus className="w-6 h-6 mx-auto mb-2 text-slate-400" />
                    <p className="text-sm">Drag additional fields here</p>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}