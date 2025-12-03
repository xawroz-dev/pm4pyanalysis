import React from 'react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue
} from '@/components/ui/select';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Briefcase, FileText, Shield } from 'lucide-react';

const prcGroups = [
    'Finance Operations',
    'Supply Chain',
    'Human Resources',
    'Customer Service',
    'IT Operations',
    'Compliance',
    'Risk Management',
];

export default function UseCaseForm({ data, onChange }) {
    const [name, setName] = React.useState(data.name || '');
    const [description, setDescription] = React.useState(data.description || '');

    React.useEffect(() => {
        setName(data.name || '');
        setDescription(data.description || '');
    }, [data.name, data.description]);

    const handleNameChange = (e) => {
        setName(e.target.value);
        onChange({ ...data, name: e.target.value });
    };

    const handleDescriptionChange = (e) => {
        setDescription(e.target.value);
        onChange({ ...data, description: e.target.value });
    };

    return (
        <Card className="border-0 shadow-xl shadow-slate-200/50">
            <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-600 to-indigo-600 flex items-center justify-center">
                        <Briefcase className="w-5 h-5 text-white" />
                    </div>
                    Define Your Use Case
                </CardTitle>
                <CardDescription className="mt-1">
                    Start by giving your process mining project a name and description
                </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6 pt-6">
                <div className="space-y-2">
                    <Label htmlFor="name" className="flex items-center gap-2">
                        <Briefcase className="w-4 h-4 text-slate-400" />
                        Use Case Name
                    </Label>
                    <Input
                        id="name"
                        placeholder="e.g., Order-to-Cash Process Analysis"
                        value={name}
                        onChange={handleNameChange}
                        className="h-12 text-base"
                    />
                </div>

                <div className="space-y-2">
                    <Label htmlFor="description" className="flex items-center gap-2">
                        <FileText className="w-4 h-4 text-slate-400" />
                        Description
                    </Label>
                    <Textarea
                        id="description"
                        placeholder="Describe what you want to analyze with this use case..."
                        value={description}
                        onChange={handleDescriptionChange}
                        className="min-h-[120px] text-base resize-none"
                    />
                </div>

                <div className="space-y-2">
                    <Label htmlFor="prc" className="flex items-center gap-2">
                        <Shield className="w-4 h-4 text-slate-400" />
                        PRC Group (Access Control)
                    </Label>
                    <Select
                        value={data.prc_group || ''}
                        onValueChange={(value) => onChange({ ...data, prc_group: value })}
                    >
                        <SelectTrigger className="h-12">
                            <SelectValue placeholder="Select a PRC group" />
                        </SelectTrigger>
                        <SelectContent>
                            {prcGroups.map((group) => (
                                <SelectItem key={group} value={group}>
                                    {group}
                                </SelectItem>
                            ))}
                        </SelectContent>
                    </Select>
                    <p className="text-xs text-slate-500 mt-1">
                        Only users in this PRC group will have access to this use case
                    </p>
                </div>
            </CardContent>
        </Card>
    );
}