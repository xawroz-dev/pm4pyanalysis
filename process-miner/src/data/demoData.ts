
import { Users, Clock, TrendingUp, Activity, AlertTriangle, ShieldAlert, CheckCircle2, XCircle, Shield, AlertCircle } from 'lucide-react';

export const DEMO_SCENARIOS = {
    CREDIT: 'credit',
    AGENTS: 'agents'
};

export const creditData = {
    useCase: {
        name: 'Credit Increase Approval Process',
        description: 'End-to-end analysis of the credit limit increase request process across Core Banking, CRM, and Risk Management systems.',
        prc_group: 'Risk Management',
    },
    applications: [
        {
            id: '1',
            name: 'Core Banking System',
            data_source: 'database',
            status: 'configured',
            parsed_fields: [
                { name: 'event_timestamp', type: 'datetime', sample: '2024-01-15T10:30:00Z' },
                { name: 'request_id', type: 'string', sample: 'REQ-001234' },
                { name: 'activity_code', type: 'string', sample: 'CREDIT_REQ_SUBMIT' },
                { name: 'customer_id', type: 'string', sample: 'CUST-9012' },
                { name: 'credit_amount', type: 'number', sample: '25000.00' },
                { name: 'current_limit', type: 'number', sample: '10000.00' },
                { name: 'requested_limit', type: 'number', sample: '35000.00' },
            ],
            field_mappings: {
                timestamp: 'event_timestamp',
                case_id: 'request_id',
                activity: 'activity_code',
            },
            additional_fields: [
                { name: 'customer_id', type: 'string' },
                { name: 'credit_amount', type: 'number' },
            ],
            stitching_key: 'customer_id',
        },
        {
            id: '2',
            name: 'CRM System',
            data_source: 'api',
            status: 'configured',
            parsed_fields: [
                { name: 'timestamp', type: 'datetime', sample: '2024-01-15T10:35:00Z' },
                { name: 'case_number', type: 'string', sample: 'CRM-56789' },
                { name: 'action_type', type: 'string', sample: 'CUSTOMER_CONTACT' },
                { name: 'client_id', type: 'string', sample: 'CUST-9012' },
                { name: 'agent_id', type: 'string', sample: 'AGT-123' },
            ],
            field_mappings: {
                timestamp: 'timestamp',
                case_id: 'case_number',
                activity: 'action_type',
            },
            additional_fields: [
                { name: 'client_id', type: 'string' },
                { name: 'agent_id', type: 'string' },
            ],
            stitching_key: 'client_id',
        },
        {
            id: '3',
            name: 'Risk Management System',
            data_source: 'file_upload',
            status: 'configured',
            parsed_fields: [
                { name: 'event_time', type: 'datetime', sample: '2024-01-15T11:00:00Z' },
                { name: 'assessment_id', type: 'string', sample: 'RISK-2024-001' },
                { name: 'check_type', type: 'string', sample: 'CREDIT_SCORE_CHECK' },
                { name: 'customer_ref', type: 'string', sample: 'CUST-9012' },
                { name: 'risk_score', type: 'number', sample: '720' },
                { name: 'risk_level', type: 'string', sample: 'MEDIUM' },
            ],
            field_mappings: {
                timestamp: 'event_time',
                case_id: 'assessment_id',
                activity: 'check_type',
            },
            additional_fields: [
                { name: 'customer_ref', type: 'string' },
                { name: 'risk_score', type: 'number' },
                { name: 'risk_level', type: 'string' },
            ],
            stitching_key: 'customer_ref',
        },
    ],
    stitchingConfig: {
        '1': 'customer_id',
        '2': 'client_id',
        '3': 'customer_ref',
    },
    stitchingJourney: [
        { app: 'CRM System', activity: 'Credit Request Submitted', time: '10:00 AM' },
        { app: 'CRM System', activity: 'Initial Review', time: '10:05 AM' },
        { app: 'Core Banking', activity: 'Credit Score Check', time: '10:15 AM' },
        { app: 'Risk Management', activity: 'Risk Assessment', time: '10:30 AM' },
        { app: 'Risk Management', activity: 'Manager Approval', time: '11:00 AM' },
        { app: 'Core Banking', activity: 'Credit Limit Set', time: '11:05 AM' },
        { app: 'CRM System', activity: 'Notification Sent', time: '11:10 AM' },
    ],
    aiNaming: [
        { original: 'CRD_INC_REQ_SUBMIT', aiSuggestion: 'Credit Increase Application Submitted', category: 'Initiation' },
        { original: 'INIT_VAL_CHK', aiSuggestion: 'Document Completeness Verification', category: 'Validation' },
        { original: 'CRED_SCORE_FETCH', aiSuggestion: 'Bureau Credit Score Retrieval', category: 'Assessment' },
        { original: 'RISK_ASSESS_START', aiSuggestion: 'Automated Risk Model Execution', category: 'Assessment' },
        { original: 'RISK_SCORE_CALC', aiSuggestion: 'Composite Risk Score Generation', category: 'Assessment' },
        { original: 'MGR_APPR_PEND', aiSuggestion: 'Branch Manager Review Queue Entry', category: 'Approval' },
        { original: 'MGR_APPR_COMP', aiSuggestion: 'Branch Manager Decision Recorded', category: 'Approval' },
        { original: 'SNR_APPR_REQ', aiSuggestion: 'Regional Director Escalation Triggered', category: 'Approval' },
        { original: 'SNR_APPR_COMP', aiSuggestion: 'Regional Director Authorization Granted', category: 'Approval' },
        { original: 'CRD_LMT_UPD', aiSuggestion: 'Account Credit Limit Modification', category: 'Completion' },
        { original: 'CUST_NOTIF_SENT', aiSuggestion: 'Customer Email Confirmation Dispatched', category: 'Communication' },
        { original: 'REQ_DENY_PROC', aiSuggestion: 'Application Rejection Finalized', category: 'Completion' },
    ],
    combineEvents: {
        activities: [
            { id: 1, name: 'Credit Score Request Sent', group: null },
            { id: 2, name: 'Credit Score Response Received', group: null },
            { id: 3, name: 'Credit Score Validated', group: null },
            { id: 4, name: 'Risk Model Invoked', group: null },
            { id: 5, name: 'Risk Score Generated', group: null },
            { id: 6, name: 'Risk Category Assigned', group: null },
            { id: 7, name: 'Manager Queue Assignment', group: null },
            { id: 8, name: 'Manager Review Started', group: null },
            { id: 9, name: 'Manager Decision Recorded', group: null },
            { id: 10, name: 'Manager Approval Notification', group: null },
            { id: 11, name: 'Senior Queue Assignment', group: null },
            { id: 12, name: 'Senior Review Started', group: null },
            { id: 13, name: 'Senior Decision Recorded', group: null },
            { id: 14, name: 'Limit Update Initiated', group: null },
            { id: 15, name: 'Limit Update Confirmed', group: null },
        ],
        suggestions: [
            { name: 'Credit Score Check', activityIds: [1, 2, 3], confidence: 96 },
            { name: 'Risk Assessment', activityIds: [4, 5, 6], confidence: 94 },
            { name: 'Manager Approval', activityIds: [7, 8, 9, 10], confidence: 91 },
            { name: 'Senior Approval', activityIds: [11, 12, 13], confidence: 89 },
            { name: 'Limit Update', activityIds: [14, 15], confidence: 97 },
        ]
    },
    labeling: {
        activities: [
            { id: 1, name: 'Credit Request Submitted' },
            { id: 2, name: 'Initial Review' },
            { id: 3, name: 'Credit Score Check' },
            { id: 4, name: 'Risk Assessment' },
            { id: 5, name: 'Manager Approval' },
            { id: 6, name: 'Senior Approval' },
            { id: 7, name: 'Credit Limit Set' },
            { id: 8, name: 'Notification Sent' },
            { id: 9, name: 'Account Updated' },
            { id: 10, name: 'Request Denied' },
        ],
        hierarchy: [
            {
                id: 'init', name: 'Request Initiation', expanded: true,
                children: [
                    { id: 'init-1', name: 'Credit Request Submitted' },
                    { id: 'init-2', name: 'Initial Review' },
                ]
            },
            {
                id: 'assessment', name: 'Assessment & Verification', expanded: true,
                children: [
                    { id: 'assess-1', name: 'Credit Score Check' },
                    { id: 'assess-2', name: 'Risk Assessment' },
                ]
            },
            {
                id: 'approval', name: 'Approval Process', expanded: true,
                children: [
                    { id: 'approv-1', name: 'Manager Approval' },
                    { id: 'approv-2', name: 'Senior Approval' },
                ]
            },
            {
                id: 'completion', name: 'Completion', expanded: true,
                children: [
                    { id: 'comp-1', name: 'Credit Limit Set' },
                    { id: 'comp-2', name: 'Notification Sent' },
                    { id: 'comp-3', name: 'Account Updated' },
                    { id: 'comp-4', name: 'Request Denied' },
                ]
            },
        ]
    },
    processNodes: [
        { id: 'start', type: 'start', label: 'Start', x: 50, y: 180 },
        { id: 'submit', type: 'task', label: 'Request\nSubmitted', x: 150, y: 180, duration: '0s', frequency: 100 },
        { id: 'validate', type: 'task', label: 'Initial\nValidation', x: 280, y: 180, duration: '5m', frequency: 100 },
        { id: 'credit_check', type: 'task', label: 'Credit\nScore Check', x: 410, y: 180, duration: '2m', frequency: 98 },
        { id: 'gateway1', type: 'gateway', label: 'Amount?', x: 540, y: 180 },
        { id: 'risk_assess', type: 'task', label: 'Risk\nAssessment', x: 670, y: 100, duration: '45m', frequency: 42 },
        { id: 'mgr_approval', type: 'task', label: 'Manager\nApproval', x: 800, y: 180, duration: '4h', frequency: 85 },
        { id: 'gateway2', type: 'gateway', label: 'Approved?', x: 930, y: 180 },
        { id: 'senior_approval', type: 'task', label: 'Senior\nApproval', x: 800, y: 60, duration: '8h', frequency: 25 },
        { id: 'set_limit', type: 'task', label: 'Credit Limit\nUpdated', x: 1060, y: 140, duration: '1m', frequency: 82 },
        { id: 'deny', type: 'task', label: 'Request\nDenied', x: 1060, y: 240, duration: '1m', frequency: 18 },
        { id: 'notify', type: 'task', label: 'Customer\nNotified', x: 1190, y: 180, duration: '0s', frequency: 100 },
        { id: 'end', type: 'end', label: 'End', x: 1290, y: 180 },
    ],
    connections: [
        { from: 'start', to: 'submit', count: 7291 },
        { from: 'submit', to: 'validate', count: 7291 },
        { from: 'validate', to: 'credit_check', count: 7150 },
        { from: 'credit_check', to: 'gateway1', count: 7150 },
        { from: 'gateway1', to: 'risk_assess', count: 3060, label: '>$10K' },
        { from: 'gateway1', to: 'mgr_approval', count: 4090, label: '≤$10K' },
        { from: 'risk_assess', to: 'mgr_approval', count: 3060 },
        { from: 'risk_assess', to: 'senior_approval', count: 1876 },
        { from: 'senior_approval', to: 'gateway2', count: 1876 },
        { from: 'mgr_approval', to: 'gateway2', count: 5270 },
        { from: 'gateway2', to: 'set_limit', count: 5980, label: 'Yes' },
        { from: 'gateway2', to: 'deny', count: 1311, label: 'No' },
        { from: 'set_limit', to: 'notify', count: 5980 },
        { from: 'deny', to: 'notify', count: 1311 },
        { from: 'notify', to: 'end', count: 7291 },
    ],
    metrics: [
        { label: 'Total Cases', value: '7,291', icon: Users, color: 'text-blue-600', bg: 'bg-blue-50' },
        { label: 'Avg. Duration', value: '2.8 days', icon: Clock, color: 'text-emerald-600', bg: 'bg-emerald-50' },
        { label: 'Approval Rate', value: '82%', icon: TrendingUp, color: 'text-violet-600', bg: 'bg-violet-50' },
        { label: 'Active Variants', value: '8', icon: Activity, color: 'text-amber-600', bg: 'bg-amber-50' },
    ],
    variantGroups: [
        {
            name: 'Credit Increase - Standard',
            totalCases: 4500,
            variants: [
                { id: 'CIA-01', activities: ['Submit', 'Validate', 'Credit Check', 'Manager Approval', 'Set Limit', 'Notify'], caseCount: 2500, percentage: 33.3, avgDuration: '2.3 days' },
                { id: 'CIA-02', activities: ['Submit', 'Validate', 'Credit Check', 'Risk Assessment', 'Manager Approval', 'Set Limit', 'Notify'], caseCount: 1500, percentage: 20.0, avgDuration: '3.1 days' },
                { id: 'CIA-03', activities: ['Submit', 'Validate', 'Credit Check', 'Set Limit', 'Notify'], caseCount: 500, percentage: 6.7, avgDuration: '1.1 days', isNew: true },
            ],
        },
        {
            name: 'Credit Increase - High Value',
            totalCases: 2000,
            variants: [
                { id: 'CIH-01', activities: ['Submit', 'Validate', 'Credit Check', 'Risk Assessment', 'Manager Approval', 'Senior Approval', 'Set Limit', 'Notify'], caseCount: 1500, percentage: 20.0, avgDuration: '4.5 days' },
                { id: 'CIH-02', activities: ['Submit', 'Validate', 'Credit Check', 'Risk Assessment', 'Legal Review', 'Manager Approval', 'Senior Approval', 'Set Limit', 'Notify'], caseCount: 500, percentage: 6.7, avgDuration: '6.2 days', isNew: true },
            ],
        },
        {
            name: 'Exceptions & Rework',
            totalCases: 800,
            variants: [
                { id: 'EXC-01', activities: ['Submit', 'Validate', 'Request More Info', 'Submit', 'Validate', 'Credit Check', 'Manager Approval', 'Set Limit', 'Notify'], caseCount: 300, percentage: 4.0, avgDuration: '5.5 days' },
                { id: 'EXC-02', activities: ['Submit', 'Validate', 'Credit Check', 'Deny', 'Notify'], caseCount: 300, percentage: 4.0, avgDuration: '0.5 days' },
                { id: 'EXC-03', activities: ['Submit', 'Validate', 'Credit Check', 'Manager Approval', 'Deny', 'Notify'], caseCount: 200, percentage: 2.7, avgDuration: '2.8 days' },
            ],
        },
        {
            name: 'Legacy Process (Manual)',
            totalCases: 200,
            variants: [
                { id: 'LEG-01', activities: ['Submit', 'Manual Review', 'Manager Approval', 'Set Limit', 'Notify'], caseCount: 200, percentage: 2.7, avgDuration: '8.5 days' },
            ],
        },
    ],
    riskViolations: [
        {
            id: 'CV-001',
            name: 'Missing Manager Approval',
            description: 'Credit increases above $10,000 processed without manager approval',
            severity: 'critical',
            violationCount: 156,
            percentOfCases: 3.2,
            status: 'identified',
            trendData: [45, 52, 38, 61, 72, 89, 102, 120, 145, 156],
            rootCause: 'System configuration error allows bypassing approval step for high-value requests during high load.',
            suggestedActions: [
                { title: 'Enforce Workflow Gate', description: 'Update process definition to make approval step mandatory for >$10k.', type: 'automation' },
                { title: 'Audit Log Review', description: 'Review logs to identify users bypassing the control.', type: 'manual' }
            ]
        },
        {
            id: 'CV-002',
            name: 'Segregation of Duties Violation',
            description: 'Same user performing credit check and approval',
            severity: 'high',
            violationCount: 89,
            percentOfCases: 1.8,
            status: 'identified',
            trendData: [12, 18, 25, 35, 42, 55, 68, 75, 82, 89],
            rootCause: 'User roles "Credit Analyst" and "Approver" are assigned to the same user group in 3 regions.',
            suggestedActions: [
                { title: 'Revoke Conflicting Roles', description: 'Remove "Approver" role from "Credit Analyst" group.', type: 'automation' },
                { title: 'User Access Review', description: 'Trigger a mandatory access review for all affected users.', type: 'manual' }
            ]
        },
        {
            id: 'CV-003',
            name: 'SLA Breach (>5 Days)',
            description: 'Credit increase requests taking longer than 5 days to complete',
            severity: 'medium',
            violationCount: 342,
            percentOfCases: 7.1,
            status: 'pending_approval',
            trendData: [200, 210, 250, 280, 300, 320, 342],
            rootCause: 'Bottleneck in "Risk Assessment" step due to understaffing during peak periods.',
            suggestedActions: [
                { title: 'Auto-Assign Backup Team', description: 'Automatically route excess cases to backup team when queue > 50.', type: 'automation' },
                { title: 'Adjust SLA Threshold', description: 'Temporarily increase SLA to 7 days for complex cases.', type: 'manual' }
            ]
        },
        {
            id: 'CV-004',
            name: 'Unauthorized Limit Increase',
            description: 'Final credit limit set higher than the approved amount',
            severity: 'critical',
            violationCount: 12,
            percentOfCases: 0.2,
            status: 'identified',
            trendData: [1, 2, 2, 5, 8, 10, 12],
            rootCause: 'Manual data entry error in Core Banking System not validated against Approval Decision.',
            suggestedActions: [
                { title: 'Implement Field Validation', description: 'Add validation rule: Limit <= Approved Amount.', type: 'automation' },
                { title: 'Revert Unauthorized Limits', description: 'Automatically reset limits to approved values.', type: 'automation' }
            ]
        }
    ],
    violationJourneys: {
        'CV-001': [
            {
                caseId: 'REQ-1089',
                steps: [
                    { activity: 'Credit Request Submitted', timestamp: '2024-01-15 09:23:14', user: 'customer_portal', system: 'Core Banking', status: 'completed' },
                    { activity: 'Initial Validation', timestamp: '2024-01-15 09:23:45', user: 'AUTO_SYSTEM', system: 'Core Banking', status: 'completed' },
                    { activity: 'Credit Score Check', timestamp: '2024-01-15 09:24:02', user: 'AUTO_SYSTEM', system: 'Risk Management', status: 'completed' },
                    { activity: 'Risk Assessment', timestamp: '2024-01-15 10:15:33', user: 'risk_analyst_01', system: 'Risk Management', status: 'completed' },
                    { activity: 'Manager Approval', timestamp: '-', user: '-', system: '-', status: 'skipped', violation: true },
                    { activity: 'Credit Limit Updated', timestamp: '2024-01-15 10:45:12', user: 'credit_officer_03', system: 'Core Banking', status: 'completed' },
                    { activity: 'Customer Notified', timestamp: '2024-01-15 10:45:45', user: 'AUTO_SYSTEM', system: 'CRM', status: 'completed' },
                ]
            }
        ],
        'CV-002': [
            {
                caseId: 'REQ-2201',
                steps: [
                    { activity: 'Credit Request Submitted', timestamp: '2024-01-16 14:00:00', user: 'customer_portal', system: 'Core Banking', status: 'completed' },
                    { activity: 'Credit Score Check', timestamp: '2024-01-16 14:05:00', user: 'john_doe', system: 'Risk Management', status: 'completed', violation: true },
                    { activity: 'Risk Assessment', timestamp: '2024-01-16 14:30:00', user: 'john_doe', system: 'Risk Management', status: 'completed' },
                    { activity: 'Manager Approval', timestamp: '2024-01-16 15:00:00', user: 'john_doe', system: 'Risk Management', status: 'completed', violation: true },
                    { activity: 'Credit Limit Updated', timestamp: '2024-01-16 15:10:00', user: 'system', system: 'Core Banking', status: 'completed' },
                ]
            }
        ],
        'CV-003': [
            {
                caseId: 'REQ-3055',
                steps: [
                    { activity: 'Credit Request Submitted', timestamp: '2024-01-10 09:00:00', user: 'customer_portal', system: 'Core Banking', status: 'completed' },
                    { activity: 'Initial Validation', timestamp: '2024-01-10 09:05:00', user: 'system', system: 'Core Banking', status: 'completed' },
                    { activity: 'Risk Assessment', timestamp: '2024-01-10 10:00:00', user: 'analyst_05', system: 'Risk Management', status: 'completed' },
                    { activity: 'Manager Approval', timestamp: '2024-01-16 11:00:00', user: 'manager_02', system: 'Risk Management', status: 'completed', violation: true },
                    { activity: 'Credit Limit Updated', timestamp: '2024-01-16 11:30:00', user: 'system', system: 'Core Banking', status: 'completed' },
                ]
            }
        ],
        'CV-004': [
            {
                caseId: 'REQ-4102',
                steps: [
                    { activity: 'Credit Request Submitted', timestamp: '2024-01-18 10:00:00', user: 'customer_portal', system: 'Core Banking', status: 'completed' },
                    { activity: 'Manager Approval', timestamp: '2024-01-18 11:00:00', user: 'manager_01', system: 'Risk Management', status: 'completed' },
                    { activity: 'Credit Limit Updated', timestamp: '2024-01-18 11:05:00', user: 'officer_09', system: 'Core Banking', status: 'completed', violation: true },
                    { activity: 'Customer Notified', timestamp: '2024-01-18 11:10:00', user: 'system', system: 'CRM', status: 'completed' },
                ]
            }
        ]
    }
};

export const agentsData = {
    useCase: {
        name: 'Customer Address Change - Agent Swarm',
        description: 'Multi-agent workflow handling customer address updates: Triage -> Identity -> System Update -> Card Issue -> Notification.',
        prc_group: 'Customer Service',
    },
    applications: [
        {
            id: 'a1',
            name: 'Triage Agent',
            data_source: 'streaming',
            status: 'configured',
            parsed_fields: [
                { name: 'timestamp', type: 'datetime', sample: '2024-02-10T09:00:00Z' },
                { name: 'trace_id', type: 'string', sample: 'TRC-998877' },
                { name: 'intent', type: 'string', sample: 'ADDRESS_CHANGE' },
                { name: 'confidence', type: 'number', sample: '0.98' },
            ],
            field_mappings: {
                timestamp: 'timestamp',
                case_id: 'trace_id',
                activity: 'intent',
            },
            additional_fields: [
                { name: 'confidence', type: 'number' },
            ],
            stitching_key: 'trace_id',
        },
        {
            id: 'a2',
            name: 'Identity Agent',
            data_source: 'api',
            status: 'configured',
            parsed_fields: [
                { name: 'check_time', type: 'datetime', sample: '2024-02-10T09:00:05Z' },
                { name: 'req_id', type: 'string', sample: 'TRC-998877' },
                { name: 'verification_status', type: 'string', sample: 'VERIFIED' },
                { name: 'method', type: 'string', sample: '2FA_SMS' },
            ],
            field_mappings: {
                timestamp: 'check_time',
                case_id: 'req_id',
                activity: 'verification_status',
            },
            additional_fields: [
                { name: 'method', type: 'string' },
            ],
            stitching_key: 'req_id',
        },
        {
            id: 'a3',
            name: 'System Update Agent',
            data_source: 'api',
            status: 'configured',
            parsed_fields: [
                { name: 'update_time', type: 'datetime', sample: '2024-02-10T09:00:10Z' },
                { name: 'ticket_id', type: 'string', sample: 'TRC-998877' },
                { name: 'action', type: 'string', sample: 'DB_UPDATE_SUCCESS' },
                { name: 'system', type: 'string', sample: 'CORE_CRM' },
            ],
            field_mappings: {
                timestamp: 'update_time',
                case_id: 'ticket_id',
                activity: 'action',
            },
            additional_fields: [
                { name: 'system', type: 'string' },
            ],
            stitching_key: 'ticket_id',
        },
        {
            id: 'a4',
            name: 'Card Issue Agent',
            data_source: 'api',
            status: 'configured',
            parsed_fields: [
                { name: 'issue_time', type: 'datetime', sample: '2024-02-10T09:00:15Z' },
                { name: 'ref_id', type: 'string', sample: 'TRC-998877' },
                { name: 'card_action', type: 'string', sample: 'NEW_CARD_ORDERED' },
            ],
            field_mappings: {
                timestamp: 'issue_time',
                case_id: 'ref_id',
                activity: 'card_action',
            },
            additional_fields: [],
            stitching_key: 'ref_id',
        },
        {
            id: 'a5',
            name: 'Notification Agent',
            data_source: 'api',
            status: 'configured',
            parsed_fields: [
                { name: 'sent_time', type: 'datetime', sample: '2024-02-10T09:00:20Z' },
                { name: 'trace_id', type: 'string', sample: 'TRC-998877' },
                { name: 'channel', type: 'string', sample: 'EMAIL_AND_SMS' },
                { name: 'status', type: 'string', sample: 'SENT' },
            ],
            field_mappings: {
                timestamp: 'sent_time',
                case_id: 'trace_id',
                activity: 'status',
            },
            additional_fields: [
                { name: 'channel', type: 'string' },
            ],
            stitching_key: 'trace_id',
        }
    ],
    stitchingConfig: {
        'a1': 'trace_id',
        'a2': 'req_id',
        'a3': 'ticket_id',
        'a4': 'ref_id',
        'a5': 'trace_id',
    },
    stitchingJourney: [
        { app: 'Triage Agent', activity: 'Address Change Requested', time: '09:00:00 AM' },
        { app: 'Identity Agent', activity: 'Identity Verified (2FA)', time: '09:00:05 AM' },
        { app: 'System Update Agent', activity: 'CRM Updated', time: '09:00:10 AM' },
        { app: 'Card Issue Agent', activity: 'Replacement Card Ordered', time: '09:00:15 AM' },
        { app: 'Notification Agent', activity: 'Confirmation Sent', time: '09:00:20 AM' },
    ],
    aiNaming: [
        { original: 'TRG_INT_DETECT', aiSuggestion: 'Triage: Intent Detected (Address Change)', category: 'Triage' },
        { original: 'ID_VER_SUCCESS', aiSuggestion: 'Identity: Verification Successful', category: 'Security' },
        { original: 'SYS_UPD_CRM', aiSuggestion: 'System: CRM Address Updated', category: 'Execution' },
        { original: 'SYS_UPD_BILL', aiSuggestion: 'System: Billing Address Updated', category: 'Execution' },
        { original: 'CRD_ORD_NEW', aiSuggestion: 'Card: New Card Ordered', category: 'Fulfillment' },
        { original: 'CRD_SHIP_LBL', aiSuggestion: 'Card: Shipping Label Generated', category: 'Fulfillment' },
        { original: 'NOTIF_SNT_EML', aiSuggestion: 'Notify: Email Confirmation Sent', category: 'Communication' },
        { original: 'NOTIF_SNT_SMS', aiSuggestion: 'Notify: SMS Alert Sent', category: 'Communication' },
    ],
    combineEvents: {
        activities: [
            { id: 1, name: 'Triage: Receive Message', group: null },
            { id: 2, name: 'Triage: Parse Intent', group: null },
            { id: 3, name: 'Triage: Route to Flow', group: null },
            { id: 4, name: 'Identity: Challenge Sent', group: null },
            { id: 5, name: 'Identity: Response Received', group: null },
            { id: 6, name: 'Identity: Verified', group: null },
            { id: 7, name: 'System: Lock Record', group: null },
            { id: 8, name: 'System: Update Field', group: null },
            { id: 9, name: 'System: Commit Transaction', group: null },
            { id: 10, name: 'Card: Check Inventory', group: null },
            { id: 11, name: 'Card: Allocate Stock', group: null },
            { id: 12, name: 'Card: Order Created', group: null },
        ],
        suggestions: [
            { name: 'Triage Phase', activityIds: [1, 2, 3], confidence: 98 },
            { name: 'Authentication', activityIds: [4, 5, 6], confidence: 99 },
            { name: 'Database Transaction', activityIds: [7, 8, 9], confidence: 95 },
            { name: 'Card Fulfillment', activityIds: [10, 11, 12], confidence: 92 },
        ]
    },
    labeling: {
        activities: [
            { id: 1, name: 'Triage Request' },
            { id: 2, name: 'Verify Identity' },
            { id: 3, name: 'Update CRM' },
            { id: 4, name: 'Update Billing' },
            { id: 5, name: 'Order New Card' },
            { id: 6, name: 'Notify Customer' },
        ],
        hierarchy: [
            {
                id: 'intake', name: 'Intake & Security', expanded: true,
                children: [
                    { id: 'i-1', name: 'Triage Request' },
                    { id: 'i-2', name: 'Verify Identity' },
                ]
            },
            {
                id: 'exec', name: 'System Execution', expanded: true,
                children: [
                    { id: 'e-1', name: 'Update CRM' },
                    { id: 'e-2', name: 'Update Billing' },
                ]
            },
            {
                id: 'fulfill', name: 'Fulfillment & Close', expanded: true,
                children: [
                    { id: 'f-1', name: 'Order New Card' },
                    { id: 'f-2', name: 'Notify Customer' },
                ]
            },
        ]
    },
    processNodes: [
        { id: 'start', type: 'start', label: 'Start', x: 50, y: 180 },
        { id: 'triage', type: 'task', label: 'Triage Agent:\nAnalyze', x: 180, y: 180, duration: '2s', frequency: 100 },
        { id: 'identity', type: 'task', label: 'Identity Agent:\nVerify', x: 320, y: 180, duration: '15s', frequency: 100 },
        { id: 'gateway_auth', type: 'gateway', label: 'Verified?', x: 460, y: 180 },
        { id: 'system_upd', type: 'task', label: 'System Agent:\nUpdate DB', x: 600, y: 120, duration: '5s', frequency: 95 },
        { id: 'card_issue', type: 'task', label: 'Card Agent:\nIssue Card', x: 740, y: 120, duration: '10s', frequency: 60 },
        { id: 'notify', type: 'task', label: 'Notify Agent:\nSend Conf', x: 880, y: 120, duration: '2s', frequency: 95 },
        { id: 'escalate', type: 'task', label: 'Triage Agent:\nEscalate', x: 600, y: 240, duration: '1m', frequency: 5 },
        { id: 'end', type: 'end', label: 'End', x: 1020, y: 180 },
    ],
    connections: [
        { from: 'start', to: 'triage', count: 5000 },
        { from: 'triage', to: 'identity', count: 5000 },
        { from: 'identity', to: 'gateway_auth', count: 5000 },
        { from: 'gateway_auth', to: 'system_upd', count: 4750, label: 'Yes' },
        { from: 'gateway_auth', to: 'escalate', count: 250, label: 'No' },
        { from: 'system_upd', to: 'card_issue', count: 3000, label: 'Card Req' },
        { from: 'system_upd', to: 'notify', count: 1750, label: 'No Card' },
        { from: 'card_issue', to: 'notify', count: 3000 },
        { from: 'notify', to: 'end', count: 4750 },
        { from: 'escalate', to: 'end', count: 250 },
    ],
    metrics: [
        { label: 'Total Requests', value: '5,000', icon: Users, color: 'text-blue-600', bg: 'bg-blue-50' },
        { label: 'Avg. Resolution', value: '45s', icon: Clock, color: 'text-emerald-600', bg: 'bg-emerald-50' },
        { label: 'Automation Rate', value: '95%', icon: TrendingUp, color: 'text-violet-600', bg: 'bg-violet-50' },
        { label: 'Agent Handoffs', value: '4.2', icon: Activity, color: 'text-amber-600', bg: 'bg-amber-50' },
    ],
    variantGroups: [
        {
            name: 'Standard Automation',
            totalCases: 3000,
            variants: [
                { id: 'AUTO-01', activities: ['Triage', 'Identity', 'System Update', 'Card Issue', 'Notify'], caseCount: 3000, percentage: 60.0, avgDuration: '42s' },
            ],
        },
        {
            name: 'Address Only (No Card)',
            totalCases: 1750,
            variants: [
                { id: 'AUTO-02', activities: ['Triage', 'Identity', 'System Update', 'Notify'], caseCount: 1750, percentage: 35.0, avgDuration: '30s' },
            ],
        },
        {
            name: 'Auth Failure Escalation',
            totalCases: 250,
            variants: [
                { id: 'MAN-01', activities: ['Triage', 'Identity', 'Escalate'], caseCount: 250, percentage: 5.0, avgDuration: '2m 15s' },
            ],
        },
    ],
    riskViolations: [
        {
            id: 'RV-001',
            name: 'Agent Loop Limit Exceeded',
            description: 'Triage <-> Identity agents looped more than 5 times',
            severity: 'high',
            violationCount: 120,
            percentOfCases: 2.4,
            status: 'identified',
            trendData: [10, 15, 40, 60, 80, 100, 120],
            rootCause: 'Ambiguous intent classification causing cyclic handoffs between Triage and Identity agents.',
            suggestedActions: [
                { title: 'Retrain Intent Model', description: 'Add "Identity Verification" examples to Triage Agent training data.', type: 'automation' },
                { title: 'Implement Max-Hop Circuit Breaker', description: 'Terminate flow and escalate to human after 3 loops.', type: 'automation' }
            ]
        },
        {
            id: 'RV-002',
            name: 'Unauthorized System Access',
            description: 'System Agent attempted update without verified token',
            severity: 'critical',
            violationCount: 15,
            percentOfCases: 0.3,
            status: 'mitigated',
            trendData: [2, 5, 8, 12, 15, 15, 15],
            rootCause: 'Token expiration time set lower than agent processing latency.',
            suggestedActions: [
                { title: 'Extend Token Validity', description: 'Increase system agent token TTL to 5 minutes.', type: 'automation' },
                { title: 'Security Policy Review', description: 'Review agent authentication protocols.', type: 'manual' }
            ]
        },
    ],
    violationJourneys: {
        'RV-001': [
            {
                caseId: 'TRC-5551',
                steps: [
                    { activity: 'Triage: Analyze', timestamp: '09:00:00', user: 'TriageAgent', system: 'Orchestrator', status: 'completed' },
                    { activity: 'Identity: Verify', timestamp: '09:00:02', user: 'IdentityAgent', system: 'AuthSys', status: 'completed' },
                    { activity: 'Triage: Analyze', timestamp: '09:00:04', user: 'TriageAgent', system: 'Orchestrator', status: 'completed', violation: true },
                    { activity: 'Identity: Verify', timestamp: '09:00:06', user: 'IdentityAgent', system: 'AuthSys', status: 'completed', violation: true },
                    { activity: 'Triage: Analyze', timestamp: '09:00:08', user: 'TriageAgent', system: 'Orchestrator', status: 'completed', violation: true },
                ]
            }
        ]
    }
};
