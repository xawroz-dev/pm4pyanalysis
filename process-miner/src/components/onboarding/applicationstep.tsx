import React, { useState } from 'react';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
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
  ArrowRight,
  GripVertical,
  Clock,
  Hash,
  Activity,
  List,
  Bot
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { Badge } from '@/components/ui/badge';

const dataSources = [
  { id: 'database', name: 'Database', icon: Database, description: 'Connect to SQL/NoSQL' },
  { id: 'api', name: 'API', icon: Cloud, description: 'REST or GraphQL API' },
  { id: 'file_upload', name: 'File Upload', icon: Upload, description: 'CSV, JSON, Excel' },
  { id: 'streaming', name: 'Streaming', icon: Radio, description: 'Real-time events' },
];

export default function ApplicationsStep({ applications, onChange }) {
  const [newApp, setNewApp] = useState({ name: '', data_source: '' });
  const [parsedFields, setParsedFields] = useState([]);
  const [mappings, setMappings] = useState({
    timestamp: null,
    case_id: null,
    activity: null,
    additional: []
  });
  const [draggedField, setDraggedField] = useState(null);
  const [isMappingMode, setIsMappingMode] = useState(false);

  const handleFileUpload = (e) => {
    // Simulate file parsing
    const file = e.target.files?.[0];
    if (file) {
      // In a real app, we'd parse the file here. 
      // For demo, we'll use sample fields based on the sample_logs.json structure
      const sampleFields = [
        { name: 'event_id', type: 'string', sample: 'EVT-001' },
        { name: 'timestamp', type: 'datetime', sample: '2024-01-15T10:00:00Z' },
        { name: 'case_id', type: 'string', sample: 'REQ-1001' },
        { name: 'activity', type: 'string', sample: 'Credit Increase Requested' },
        { name: 'resource', type: 'string', sample: 'System' },
        { name: 'amount', type: 'number', sample: '5000' }
      ];
      setParsedFields(sampleFields);
      setIsMappingMode(true);
    }
  };

  const handleDragStart = (e, field) => {
    setDraggedField(field);
    e.dataTransfer.setData('field', JSON.stringify(field));
  };

  const handleDrop = (e, target) => {
    e.preventDefault();
    if (!draggedField) return;

    if (target === 'additional') {
      if (!mappings.additional.find(f => f.name === draggedField.name)) {
        setMappings(prev => ({ ...prev, additional: [...prev.additional, draggedField] }));
      }
    } else {
      setMappings(prev => ({ ...prev, [target]: draggedField }));
    }
    setDraggedField(null);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
  };

  const addApplication = () => {
    if (newApp.name && newApp.data_source) {
      const appData = {
        ...newApp,
        id: Date.now().toString(),
        parsed_fields: parsedFields,
        field_mappings: {
          timestamp: mappings.timestamp?.name,
          case_id: mappings.case_id?.name,
          activity: mappings.activity?.name,
        },
        additional_fields: mappings.additional
      };
      onChange([...applications, appData]);
      setNewApp({ name: '', data_source: '' });
      setParsedFields([]);
      setMappings({ timestamp: null, case_id: null, activity: null, additional: [] });
      setIsMappingMode(false);
    }
  };

  const removeApplication = (id) => {
    onChange(applications.filter(app => app.id !== id));
  };

  return (
    <div className="space-y-6">
      <Card className="border-0 shadow-xl shadow-slate-200/50">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-600 to-teal-600 flex items-center justify-center">
              <Server className="w-5 h-5 text-white" />
            </div>
            Add Applications
          </CardTitle>
          <CardDescription className="mt-1">
            Connect multiple applications to stitch journeys across systems
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6 pt-6">

          {!isMappingMode ? (
            /* Initial Add Form */
            <div className="p-6 rounded-2xl bg-gradient-to-br from-slate-50 to-slate-100/50 border border-slate-200/50">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label>Application Name</Label>
                  <Input
                    placeholder="e.g., SAP ERP, Salesforce CRM"
                    value={newApp.name}
                    onChange={(e) => setNewApp({ ...newApp, name: e.target.value })}
                    className="h-11 bg-white"
                  />
                </div>
                <div className="space-y-2">
                  <Label>Data Source Type</Label>
                  <Select
                    value={newApp.data_source}
                    onValueChange={(value) => setNewApp({ ...newApp, data_source: value })}
                  >
                    <SelectTrigger className="h-11 bg-white">
                      <SelectValue placeholder="Select data source" />
                    </SelectTrigger>
                    <SelectContent>
                      {dataSources.map((source) => (
                        <SelectItem key={source.id} value={source.id}>
                          <div className="flex items-center gap-2">
                            <source.icon className="w-4 h-4 text-slate-500" />
                            <span>{source.name}</span>
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="mt-6 flex items-center gap-4">
                <div className="relative flex-1">
                  <div className="absolute inset-0 flex items-center">
                    <span className="w-full border-t border-slate-200" />
                  </div>
                  <div className="relative flex justify-center text-xs uppercase">
                    <span className="bg-slate-50 px-2 text-slate-500">Or upload file</span>
                  </div>
                </div>
              </div>

              <div className="mt-4 flex flex-col md:flex-row gap-4">
                <Button
                  variant="outline"
                  className="flex-1 border-dashed border-2 h-24 hover:bg-slate-50 hover:border-emerald-500 hover:text-emerald-600 transition-all"
                  onClick={() => document.getElementById('file-upload').click()}
                >
                  <div className="flex flex-col items-center gap-2">
                    <Upload className="w-6 h-6" />
                    <span>Upload Log File (JSON/CSV)</span>
                  </div>
                  <input
                    id="file-upload"
                    type="file"
                    className="hidden"
                    accept=".json,.csv"
                    onChange={handleFileUpload}
                  />
                </Button>
              </div>

              <div className="mt-4 text-center flex justify-center gap-4">
                <a href="/sample_logs.json" download className="text-xs text-blue-600 hover:underline">
                  Download credit logs
                </a>
                <span className="text-slate-300">|</span>
                <a href="/sample_agent_logs.json" download className="text-xs text-violet-600 hover:underline">
                  Download agent logs
                </a>
              </div>
            </div>
          ) : (
            /* Mapping Interface */
            <div className="space-y-6">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold text-slate-900">Map Fields for {newApp.name}</h3>
                <Button variant="ghost" size="sm" onClick={() => setIsMappingMode(false)}>Cancel</Button>
              </div>

              <div className="grid md:grid-cols-2 gap-8">
                {/* Source Fields */}
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-sm font-medium text-slate-500">Detected Fields</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {parsedFields.map((field) => (
                        <div
                          key={field.name}
                          draggable
                          onDragStart={(e) => handleDragStart(e, field)}
                          className="flex items-center justify-between p-3 rounded-lg bg-white border border-slate-200 cursor-grab active:cursor-grabbing hover:border-blue-400 hover:shadow-sm transition-all"
                        >
                          <div className="flex items-center gap-3">
                            <GripVertical className="w-4 h-4 text-slate-400" />
                            <span className="font-medium text-slate-700">{field.name}</span>
                          </div>
                          <Badge variant="secondary" className="text-xs">{field.type}</Badge>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>

                {/* Target Zones */}
                <div className="space-y-4">
                  {/* Timestamp */}
                  <div
                    onDragOver={handleDragOver}
                    onDrop={(e) => handleDrop(e, 'timestamp')}
                    className={cn(
                      "p-4 rounded-xl border-2 border-dashed transition-all",
                      mappings.timestamp ? "border-emerald-500 bg-emerald-50" : "border-slate-300 hover:border-blue-400"
                    )}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Clock className="w-4 h-4 text-slate-500" />
                        <span className="font-medium text-slate-900">Timestamp (Required)</span>
                      </div>
                      {mappings.timestamp && (
                        <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => setMappings(p => ({ ...p, timestamp: null }))}>
                          <Trash2 className="w-3 h-3 text-red-500" />
                        </Button>
                      )}
                    </div>
                    {mappings.timestamp ? (
                      <Badge className="bg-emerald-100 text-emerald-800 border-0">{mappings.timestamp.name}</Badge>
                    ) : (
                      <p className="text-sm text-slate-400">Drop timestamp field here</p>
                    )}
                  </div>

                  {/* Case ID */}
                  <div
                    onDragOver={handleDragOver}
                    onDrop={(e) => handleDrop(e, 'case_id')}
                    className={cn(
                      "p-4 rounded-xl border-2 border-dashed transition-all",
                      mappings.case_id ? "border-emerald-500 bg-emerald-50" : "border-slate-300 hover:border-blue-400"
                    )}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Hash className="w-4 h-4 text-slate-500" />
                        <span className="font-medium text-slate-900">Case ID (Required)</span>
                      </div>
                      {mappings.case_id && (
                        <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => setMappings(p => ({ ...p, case_id: null }))}>
                          <Trash2 className="w-3 h-3 text-red-500" />
                        </Button>
                      )}
                    </div>
                    {mappings.case_id ? (
                      <Badge className="bg-emerald-100 text-emerald-800 border-0">{mappings.case_id.name}</Badge>
                    ) : (
                      <p className="text-sm text-slate-400">Drop case ID field here</p>
                    )}
                  </div>

                  {/* Activity */}
                  <div
                    onDragOver={handleDragOver}
                    onDrop={(e) => handleDrop(e, 'activity')}
                    className={cn(
                      "p-4 rounded-xl border-2 border-dashed transition-all",
                      mappings.activity ? "border-emerald-500 bg-emerald-50" : "border-slate-300 hover:border-blue-400"
                    )}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center gap-2">
                        <Activity className="w-4 h-4 text-slate-500" />
                        <span className="font-medium text-slate-900">Activity Name (Required)</span>
                      </div>
                      {mappings.activity && (
                        <Button variant="ghost" size="sm" className="h-6 w-6 p-0" onClick={() => setMappings(p => ({ ...p, activity: null }))}>
                          <Trash2 className="w-3 h-3 text-red-500" />
                        </Button>
                      )}
                    </div>
                    {mappings.activity ? (
                      <Badge className="bg-emerald-100 text-emerald-800 border-0">{mappings.activity.name}</Badge>
                    ) : (
                      <p className="text-sm text-slate-400">Drop activity field here</p>
                    )}
                  </div>

                  {/* Additional Fields */}
                  <div
                    onDragOver={handleDragOver}
                    onDrop={(e) => handleDrop(e, 'additional')}
                    className="p-4 rounded-xl border-2 border-dashed border-slate-300 hover:border-blue-400 transition-all"
                  >
                    <div className="flex items-center gap-2 mb-2">
                      <List className="w-4 h-4 text-slate-500" />
                      <span className="font-medium text-slate-900">Additional Fields</span>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {mappings.additional.map((field, i) => (
                        <Badge key={i} variant="secondary" className="flex items-center gap-1">
                          {field.name}
                          <button
                            onClick={() => setMappings(p => ({ ...p, additional: p.additional.filter(f => f.name !== field.name) }))}
                            className="ml-1 hover:text-red-500"
                          >
                            <Trash2 className="w-3 h-3" />
                          </button>
                        </Badge>
                      ))}
                      {mappings.additional.length === 0 && (
                        <p className="text-sm text-slate-400">Drop other fields here</p>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              <Button
                onClick={addApplication}
                disabled={!mappings.timestamp || !mappings.case_id || !mappings.activity}
                className="w-full bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-700 hover:to-teal-700"
              >
                <CheckCircle2 className="w-4 h-4 mr-2" />
                Confirm Mapping & Add Application
              </Button>
            </div>
          )}

        </CardContent>
      </Card>

      {/* Added applications list */}
      {applications.length > 0 && (
        <Card className="border-0 shadow-xl shadow-slate-200/50">
          <CardHeader>
            <CardTitle className="text-lg">Added Applications ({applications.length})</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {applications.map((app, index) => {
                const source = dataSources.find(s => s.id === app.data_source);
                const Icon = source?.icon || Database;
                return (
                  <div
                    key={app.id}
                    className="flex items-center justify-between p-4 rounded-xl bg-gradient-to-r from-slate-50 to-white border border-slate-200/50 group hover:shadow-md transition-all"
                  >
                    <div className="flex items-center gap-4">
                      <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white font-semibold">
                        {index + 1}
                      </div>
                      <div>
                        <p className="font-medium text-slate-900">{app.name}</p>
                        <div className="flex items-center gap-2 text-sm text-slate-500">
                          <Icon className="w-3.5 h-3.5" />
                          <span>{source?.name || app.data_source}</span>
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <CheckCircle2 className="w-5 h-5 text-emerald-500" />
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => removeApplication(app.id)}
                        className="opacity-0 group-hover:opacity-100 text-slate-400 hover:text-red-500 transition-all"
                      >
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </div>
                  </div>
                );
              })}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}