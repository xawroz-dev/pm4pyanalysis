import React from 'react';
import { Check } from 'lucide-react';
import { cn } from '@/lib/utils';

const steps = [
    { id: 1, name: 'Use Case', description: 'Define your use case' },
    { id: 2, name: 'Applications', description: 'Connect & map data' },
    { id: 3, name: 'Stitching', description: 'Connect journeys' },
    { id: 4, name: 'AI Naming', description: 'Smart renaming' },
    { id: 5, name: 'Combine Events', description: 'Group activities' },
    { id: 6, name: 'Leveling', description: 'Create hierarchy' },
];

export default function StepIndicator({ currentStep, onStepClick }) {
    return (
        <div className="mb-8">
            <div className="hidden lg:flex items-center justify-between">
                {steps.map((step, index) => (
                    <React.Fragment key={step.id}>
                        <button
                            onClick={() => step.id <= currentStep && onStepClick(step.id)}
                            disabled={step.id > currentStep}
                            className={cn(
                                "flex flex-col items-center group cursor-pointer transition-all",
                                step.id > currentStep && "opacity-50 cursor-not-allowed"
                            )}
                        >
                            <div className={cn(
                                "w-10 h-10 rounded-full flex items-center justify-center transition-all duration-300",
                                step.id < currentStep
                                    ? "bg-gradient-to-br from-emerald-500 to-emerald-600 text-white shadow-lg shadow-emerald-500/30"
                                    : step.id === currentStep
                                        ? "bg-gradient-to-br from-blue-600 to-indigo-600 text-white shadow-lg shadow-blue-600/30 ring-4 ring-blue-100"
                                        : "bg-slate-100 text-slate-400"
                            )}>
                                {step.id < currentStep ? (
                                    <Check className="w-5 h-5" />
                                ) : (
                                    <span className="text-sm font-semibold">{step.id}</span>
                                )}
                            </div>
                            <span className={cn(
                                "mt-2 text-xs font-medium transition-colors",
                                step.id === currentStep ? "text-blue-600" : "text-slate-500"
                            )}>
                                {step.name}
                            </span>
                        </button>
                        {index < steps.length - 1 && (
                            <div className={cn(
                                "flex-1 h-0.5 mx-2 transition-colors duration-500",
                                step.id < currentStep ? "bg-emerald-500" : "bg-slate-200"
                            )} />
                        )}
                    </React.Fragment>
                ))}
            </div>

            {/* Mobile view */}
            <div className="lg:hidden">
                <div className="flex items-center justify-between mb-4">
                    <span className="text-sm text-slate-500">Step {currentStep} of {steps.length}</span>
                    <span className="text-sm font-medium text-blue-600">{steps[currentStep - 1]?.name}</span>
                </div>
                <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                    <div
                        className="h-full bg-gradient-to-r from-blue-600 to-indigo-600 transition-all duration-500"
                        style={{ width: `${(currentStep / steps.length) * 100}%` }}
                    />
                </div>
            </div>
        </div>
    );
}