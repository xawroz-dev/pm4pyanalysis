// @ts-nocheck
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { createPageUrl } from '../utils';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { base44 } from '@/api/base44Client';
import { Button } from '@/components/ui/button';
import { ArrowLeft, ArrowRight, CheckCircle2, Sparkles, Wand2, FileJson, Bot } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

import StepIndicator from '../components/onboarding/stepindicator';
import UseCaseForm from '../components/onboarding/usecaseform';
import ApplicationOnboarding from '../components/onboarding/applicationonboarding';
import StitchingStep from '../components/onboarding/stitchingstep';
import AINamingStep from '../components/onboarding/ainamingstep';
import CombineEventsStep from '../components/onboarding/combineeventsstep';
import LabelingStep from '../components/onboarding/labelingstep';

import { useDemo, DEMO_SCENARIOS } from '@/context/DemoContext';

export default function Onboarding() {
    const navigate = useNavigate();
    const queryClient = useQueryClient();
    const { currentDemo, setDemo, data: demoData } = useDemo();

    const [currentStep, setCurrentStep] = useState(1);
    const [useCaseData, setUseCaseData] = useState({});
    const [applications, setApplications] = useState([]);
    const [stitchingConfig, setStitchingConfig] = useState({});
    const [activities, setActivities] = useState([]);
    const [combinedGroups, setCombinedGroups] = useState({ activities: [], suggestions: [] });
    const [labelingConfig, setLabelingConfig] = useState({});

    // Effect to auto-populate when demo mode changes
    React.useEffect(() => {
        console.log('Onboarding: demoData updated', demoData);
        if (demoData) {
            console.log('Onboarding: Setting state from demoData', demoData);
            setUseCaseData(demoData.useCase || {});
            setApplications(demoData.applications || []);
            setStitchingConfig(demoData.stitchingConfig || {});
            // New data population
            setActivities(demoData.aiNaming || []);
            setCombinedGroups(demoData.combineEvents || { activities: [], suggestions: [] });
            setLabelingConfig(demoData.labeling || { activities: [], hierarchy: [] });
        }
    }, [currentDemo, demoData]);

    const handleDemoSwitch = (scenario) => {
        setDemo(scenario);
    };

    const createMutation = useMutation({
        mutationFn: async () => {
            const useCase = await base44.entities.UseCase.create({
                ...useCaseData,
                stitching_config: stitchingConfig,
                labeling_config: labelingConfig,
                status: 'active',
            });

            // Create applications
            for (const app of applications) {
                await base44.entities.Application.create({
                    name: app.name,
                    use_case_id: useCase.id,
                    data_source: app.data_source,
                    field_mappings: app.field_mappings,
                    stitching_key: app.stitching_key,
                    status: 'active',
                });
            }

            return useCase;
        },
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['useCases'] });
            navigate(createPageUrl('ProcessDiscovery'));
        },
    });

    const canProceed = () => {
        switch (currentStep) {
            case 1:
                return useCaseData.name && useCaseData.prc_group;
            case 2:
                return applications.length > 0 && applications.every(app =>
                    app.field_mappings?.timestamp && app.field_mappings?.case_id && app.field_mappings?.activity
                );
            case 3:
                return true;
            case 4:
                return true;
            case 5:
                return true;
            case 6:
                return true;
            default:
                return true;
        }
    };

    const nextStep = () => {
        if (currentStep < 6) {
            setCurrentStep(currentStep + 1);
        } else {
            createMutation.mutate();
        }
    };

    const prevStep = () => {
        if (currentStep > 1) {
            setCurrentStep(currentStep - 1);
        }
    };

    const renderStep = () => {
        switch (currentStep) {
            case 1:
                return <UseCaseForm key={currentDemo} data={useCaseData} onChange={setUseCaseData} />;
            case 2:
                return (
                    <ApplicationOnboarding
                        key={currentDemo}
                        applications={applications}
                        onChange={setApplications}
                    />
                );
            case 3:
                return (
                    <StitchingStep
                        key={currentDemo}
                        applications={applications}
                        stitchingConfig={stitchingConfig}
                        onChange={setStitchingConfig}
                        sampleJourney={demoData?.stitchingJourney}
                    />
                );
            case 4:
                return <AINamingStep key={currentDemo} initialActivities={activities} onUpdate={setActivities} />;
            case 5:
                return (
                    <CombineEventsStep
                        key={currentDemo}
                        initialActivities={combinedGroups.activities}
                        aiSuggestions={combinedGroups.suggestions}
                        onChange={setCombinedGroups}
                    />
                );
            case 6:
                return (
                    <LabelingStep
                        key={currentDemo}
                        initialActivities={labelingConfig.activities}
                        aiHierarchy={labelingConfig.hierarchy}
                        onChange={setLabelingConfig}
                    />
                );
            default:
                return null;
        }
    };

    return (
        <div className="max-w-6xl mx-auto">
            {/* Header */}
            <div className="mb-8 flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-600 to-indigo-600 flex items-center justify-center">
                        <Sparkles className="w-5 h-5 text-white" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-slate-900">Process Mining Onboarding</h1>
                        <p className="text-slate-500">Set up your use case step by step</p>
                    </div>
                </div>
                <div className="flex gap-2">
                    <Button
                        onClick={() => handleDemoSwitch(DEMO_SCENARIOS.CREDIT)}
                        variant={currentDemo === DEMO_SCENARIOS.CREDIT ? "default" : "outline"}
                        className={currentDemo === DEMO_SCENARIOS.CREDIT ? "bg-blue-600 hover:bg-blue-700" : "border-blue-200 text-blue-700 hover:bg-blue-50"}
                    >
                        <FileJson className="w-4 h-4 mr-2" />
                        Demo: Credit Increase
                    </Button>
                    <Button
                        onClick={() => handleDemoSwitch(DEMO_SCENARIOS.AGENTS)}
                        variant={currentDemo === DEMO_SCENARIOS.AGENTS ? "default" : "outline"}
                        className={currentDemo === DEMO_SCENARIOS.AGENTS ? "bg-violet-600 hover:bg-violet-700" : "border-violet-200 text-violet-700 hover:bg-violet-50"}
                    >
                        <Bot className="w-4 h-4 mr-2" />
                        Demo: Monitor Agents
                    </Button>
                </div>
            </div>

            {/* Step Indicator */}
            <StepIndicator currentStep={currentStep} onStepClick={setCurrentStep} />

            {/* Step Content */}
            <AnimatePresence mode="wait">
                <motion.div
                    key={currentStep}
                    initial={{ opacity: 0, x: 20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                    transition={{ duration: 0.3 }}
                >
                    {renderStep()}
                </motion.div>
            </AnimatePresence>

            {/* Navigation */}
            <div className="flex items-center justify-between mt-8 pt-6 border-t border-slate-200">
                <Button
                    variant="outline"
                    onClick={prevStep}
                    disabled={currentStep === 1}
                    className="gap-2"
                >
                    <ArrowLeft className="w-4 h-4" />
                    Previous
                </Button>

                <div className="flex items-center gap-2">
                    <span className="text-sm text-slate-500">Step {currentStep} of 6</span>
                </div>

                <Button
                    onClick={nextStep}
                    disabled={!canProceed() || createMutation.isPending}
                    className="gap-2 bg-gradient-to-r from-blue-600 to-indigo-600 hover:from-blue-700 hover:to-indigo-700"
                >
                    {currentStep === 6 ? (
                        <>
                            <CheckCircle2 className="w-4 h-4" />
                            {createMutation.isPending ? 'Creating...' : 'Complete Setup'}
                        </>
                    ) : (
                        <>
                            Next
                            <ArrowRight className="w-4 h-4" />
                        </>
                    )}
                </Button>
            </div>
        </div>
    );
}