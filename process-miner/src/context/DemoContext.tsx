
import React, { createContext, useContext, useState, useEffect } from 'react';
import { DEMO_SCENARIOS, creditData, agentsData } from '@/data/demoData';

const DemoContext = createContext({
    currentDemo: DEMO_SCENARIOS.CREDIT,
    setDemo: (demo) => { },
    data: creditData as any,
});

export const DemoProvider = ({ children }) => {
    const [currentDemo, setCurrentDemo] = useState(() => {
        return localStorage.getItem('processMiner_demoMode') || DEMO_SCENARIOS.CREDIT;
    });

    useEffect(() => {
        localStorage.setItem('processMiner_demoMode', currentDemo);
    }, [currentDemo]);

    const data = currentDemo === DEMO_SCENARIOS.AGENTS ? agentsData : creditData;

    return (
        <DemoContext.Provider value={{ currentDemo, setDemo: setCurrentDemo, data }}>
            {children}
        </DemoContext.Provider>
    );
};

export { DEMO_SCENARIOS };
export const useDemo = () => useContext(DemoContext);
