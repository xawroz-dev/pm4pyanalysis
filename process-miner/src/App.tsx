import React from 'react';
import { HashRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

// Import pages
import Dashboard from './pages/Dashboard';
import Onboarding from './pages/onboarding';
import ProcessDiscovery from './pages/processdiscovery';
import RiskControl from './pages/riskcontrol';
import VariantManagement from './pages/variantmanagement';

import Layout from './components/layout/Layout';

const queryClient = new QueryClient();

import { DemoProvider } from './context/DemoContext';

function App() {
    return (
        <QueryClientProvider client={queryClient}>
            <DemoProvider>
                <Router>
                    <Layout>
                        <Routes>
                            <Route path="/" element={<Navigate to="/dashboard" replace />} />
                            <Route path="/dashboard" element={<Dashboard />} />
                            <Route path="/onboarding" element={<Onboarding />} />
                            <Route path="/processdiscovery" element={<ProcessDiscovery />} />
                            <Route path="/riskcontrols" element={<RiskControl />} />
                            <Route path="/variantmanagement" element={<VariantManagement />} />
                        </Routes>
                    </Layout>
                </Router>
            </DemoProvider>
        </QueryClientProvider>
    );
}

export default App;
