export const base44 = {
    entities: {
        UseCase: {
            create: async (data: any) => {
                console.log('Creating UseCase:', data);
                await new Promise(resolve => setTimeout(resolve, 1000));
                return { id: 'mock-use-case-id', ...data };
            },
            list: async () => {
                console.log('Listing UseCases');
                return [
                    { id: '1', name: 'Credit Increase', status: 'active', prc_group: 'Risk Management', created_at: '2024-01-15' },
                    { id: '2', name: 'Loan Application', status: 'draft', prc_group: 'Lending', created_at: '2024-01-20' }
                ];
            }
        },
        Application: {
            create: async (data: any) => {
                console.log('Creating Application:', data);
                return { id: 'mock-app-id', ...data };
            },
            list: async () => {
                return [];
            }
        }
    }
};
