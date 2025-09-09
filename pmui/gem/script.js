document.addEventListener('DOMContentLoaded', () => {
    // --- Mock Data ---
    const variantsData = [
        { id: 1, group: 'Happy Path', cases: 1250, avgDuration: '3d 4h', activities: ['Create Order', 'Check Stock', 'Approve Order', 'Ship Goods', 'Send Invoice'] },
        { id: 2, group: 'Happy Path', cases: 830, avgDuration: '3d 8h', activities: ['Create Order', 'Approve Order', 'Ship Goods', 'Send Invoice'] },
        { id: 3, group: 'Rejection Path', cases: 420, avgDuration: '1d 2h', activities: ['Create Order', 'Check Stock', 'Reject Order'] },
        { id: 4, group: 'Complex Path', cases: 150, avgDuration: '7d 1h', activities: ['Create Order', 'Check Stock', 'Approve Order', 'Check Stock', 'Ship Goods', 'Send Invoice'] },
        { id: 5, group: 'Rejection Path', cases: 95, avgDuration: '1d 12h', activities: ['Create Order', 'Reject Order'] },
    ];

    // --- Extract unique activities for auto-suggestion ---
    const allActivities = [...new Set(variantsData.flatMap(v => v.activities))];

    // --- DOM Elements ---
    const container = document.getElementById('variant-list-container');
    const panel = document.getElementById('drilldown-panel');
    const panelContent = document.getElementById('drilldown-content');
    const closePanelBtn = document.getElementById('close-panel-btn');
    const applyFilterBtn = document.getElementById('applyFilterBtn');
    const filterBuilder = document.getElementById('filter-builder');
    const addFilterGroupBtn = document.getElementById('addFilterGroupBtn');

    let currentChart = null;

    // --- Core Rendering Functions ---
    function renderVariants(data) {
        container.innerHTML = '';
        if (data.length === 0) {
            container.innerHTML = '<p style="text-align:center; padding: 2rem; color: var(--text-secondary);">No variants match the current filters.</p>';
            return;
        }
        const groupedData = data.reduce((acc, variant) => {
            (acc[variant.group] = acc[variant.group] || []).push(variant);
            return acc;
        }, {});

        for (const groupName in groupedData) {
            const groupVariants = groupedData[groupName];
            const groupCases = groupVariants.reduce((sum, v) => sum + v.cases, 0);

            const groupEl = document.createElement('div');
            groupEl.className = 'variant-group';
            groupEl.innerHTML = `
                <div class="variant-group-header" data-group="${groupName}">
                    <span class="group-title">${groupName}</span>
                    <span class="group-stats">${groupVariants.length} variants • ${groupCases.toLocaleString()} cases</span>
                </div>
                <div class="variant-group-content">
                    ${groupVariants.map(variant => createVariantCardHTML(variant)).join('')}
                </div>
            `;
            container.appendChild(groupEl);
        }
    }

    function createVariantCardHTML(variant) {
        const activityFlowHTML = variant.activities
            .map(act => `<span class="activity-pill" data-activity="${act}">${act}</span>`)
            .join('<span class="arrow">&rarr;</span>');

        return `
            <div class="variant-card" data-variant-id="${variant.id}">
                <div class="variant-info">
                    <div class="activity-flow">${activityFlowHTML}</div>
                    <div class="case-count">${variant.cases.toLocaleString()} cases</div>
                </div>
                <div class="variant-metric">${variant.avgDuration}</div>
                <div class="variant-actions">
                    <input type="checkbox" class="variant-checkbox" data-variant-id="${variant.id}">
                </div>
            </div>
        `;
    }

    function showDrilldown(variantId) {
        const variant = variantsData.find(v => v.id === parseInt(variantId));
        if (!variant) return;

        panelContent.innerHTML = `
            <h2>Details for Variant #${variant.id}</h2>
            <p><strong>Path:</strong> ${variant.activities.join(' → ')}</p>
            <hr>
            <h3>Performance Metrics</h3>
            <p><strong>Total Cases:</strong> ${variant.cases.toLocaleString()}</p>
            <p><strong>Average Duration:</strong> ${variant.avgDuration}</p>
            
            <div class="chart-container">
                <canvas id="metricsChart"></canvas>
            </div>

            <h3>Case List</h3>
            <table class="data-table">
                <thead>
                    <tr><th>Case ID</th><th>Start Time</th><th>End Time</th></tr>
                </thead>
                <tbody>
                    <tr><td>CASE-1023</td><td>2025-09-01 10:00</td><td>2025-09-04 14:00</td></tr>
                    <tr><td>CASE-1029</td><td>2025-09-01 11:30</td><td>2025-09-04 19:30</td></tr>
                    <tr><td>CASE-1045</td><td>2025-09-02 09:00</td><td>2025-09-05 11:00</td></tr>
                </tbody>
            </table>
        `;

        if (currentChart) { currentChart.destroy(); }

        const ctx = document.getElementById('metricsChart').getContext('2d');
        currentChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['This Variant', 'Other Variants'],
                datasets: [{
                    label: 'Case Distribution',
                    data: [variant.cases, variantsData.reduce((sum, v) => sum + v.cases, 0) - variant.cases],
                    backgroundColor: ['#3b82f6', '#e2e8f0'],
                    borderColor: ['#ffffff'],
                    borderWidth: 2
                }]
            },
            options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' }, title: { display: true, text: 'Case Volume Contribution' } } }
        });

        panel.classList.add('open');
    }

    // --- Filter Logic ---
    function createFilterRuleHTML() {
        return `
            <div class="filter-rule">
                <select class="rule-logic">
                    <option value="contains">Contains</option>
                    <option value="starts_with">Starts With</option>
                    <option value="ends_with">Ends With</option>
                    <option value="directly_follows">Directly Follows</option>
                </select>
                <div class="autosuggest-wrapper">
                    <input type="text" class="rule-value" placeholder="e.g., 'Approve Order' or 'Ship->Invoice'" autocomplete="off">
                    <div class="suggestions-container"></div>
                </div>
                <div class="rule-actions">
                    <button class="remove-rule-btn">- Remove</button>
                </div>
            </div>
        `;
    }

    function addFilterGroup() {
        if (filterBuilder.children.length > 0) {
            const separator = document.createElement('div');
            separator.className = 'or-separator';
            separator.innerHTML = '<span>OR</span>';
            filterBuilder.appendChild(separator);
        }

        const groupContainer = document.createElement('div');
        groupContainer.className = 'filter-group-container';
        groupContainer.innerHTML = `
            <div class="group-rules">
                ${createFilterRuleHTML()}
            </div>
            <div class="group-actions">
                <button class="add-rule-btn">+ Add AND Condition</button>
            </div>
        `;
        filterBuilder.appendChild(groupContainer);
    }

    function parseAndApplyFilters() {
        const filterGroups = [];
        document.querySelectorAll('.filter-group-container').forEach(groupEl => {
            const rules = [];
            groupEl.querySelectorAll('.filter-rule').forEach(ruleEl => {
                const logic = ruleEl.querySelector('.rule-logic').value;
                const value = ruleEl.querySelector('.rule-value').value.toLowerCase().trim();
                if (value) {
                    rules.push({ logic, value });
                }
            });
            if (rules.length > 0) {
                filterGroups.push(rules);
            }
        });

        if (filterGroups.length === 0) {
            renderVariants(variantsData);
            return;
        }

        const filtered = variantsData.filter(variant => {
            const activities = variant.activities.map(a => a.toLowerCase());
            // OR logic between groups
            return filterGroups.some(rules => {
                // AND logic within a group
                return rules.every(rule => {
                    switch (rule.logic) {
                        case 'contains':
                            return activities.some(act => act.includes(rule.value));
                        case 'starts_with':
                            return activities.length > 0 && activities[0].includes(rule.value);
                        case 'ends_with':
                            return activities.length > 0 && activities[activities.length - 1].includes(rule.value);
                        case 'directly_follows':
                            const terms = rule.value.split('->').map(t => t.trim());
                            if (terms.length !== 2) return false;
                            for (let i = 0; i < activities.length - 1; i++) {
                                if (activities[i].includes(terms[0]) && activities[i + 1].includes(terms[1])) {
                                    return true;
                                }
                            }
                            return false;
                        default: return false;
                    }
                });
            });
        });

        renderVariants(filtered);
    }

    // --- Auto-Suggestion & Filter Builder Event Handlers ---
    filterBuilder.addEventListener('input', (e) => {
        if (e.target.classList.contains('rule-value')) {
            const input = e.target;
            const suggestionsContainer = input.nextElementSibling;
            const value = input.value.toLowerCase();

            if (value.length > 0 && !value.includes('->')) {
                const filteredActivities = allActivities.filter(activity =>
                    activity.toLowerCase().includes(value)
                );

                if (filteredActivities.length > 0) {
                    suggestionsContainer.innerHTML = filteredActivities.map(activity => {
                        const regex = new RegExp(value, 'gi');
                        const highlighted = activity.replace(regex, (match) => `<strong>${match}</strong>`);
                        return `<div class="suggestion-item">${highlighted}</div>`;
                    }).join('');
                    suggestionsContainer.style.display = 'block';
                } else {
                    suggestionsContainer.style.display = 'none';
                }
            } else {
                suggestionsContainer.style.display = 'none';
            }
        }
    });

    filterBuilder.addEventListener('click', (e) => {
        // Handle clicking on a suggestion item
        if (e.target.classList.contains('suggestion-item')) {
            const wrapper = e.target.closest('.autosuggest-wrapper');
            const input = wrapper.querySelector('.rule-value');
            input.value = e.target.textContent;
            wrapper.querySelector('.suggestions-container').style.display = 'none';
        }

        // Handle add/remove buttons for filter rules
        if (e.target.classList.contains('add-rule-btn')) {
            const rulesContainer = e.target.parentElement.previousElementSibling;
            rulesContainer.insertAdjacentHTML('beforeend', createFilterRuleHTML());
        }
        if (e.target.classList.contains('remove-rule-btn')) {
            const ruleElement = e.target.closest('.filter-rule');
            const groupContainer = e.target.closest('.filter-group-container');
            const rules = groupContainer.querySelectorAll('.filter-rule');

            if (rules.length > 1) {
                ruleElement.remove();
            } else {
                const orSeparator = groupContainer.previousElementSibling;
                if(orSeparator && orSeparator.classList.contains('or-separator')) {
                    orSeparator.remove();
                } else {
                    const nextSeparator = groupContainer.nextElementSibling;
                     if(nextSeparator && nextSeparator.classList.contains('or-separator')) {
                        nextSeparator.remove();
                    }
                }
                groupContainer.remove();
            }
        }
    });

    document.addEventListener('click', (e) => {
        // Hide suggestions when clicking anywhere else
        if (!e.target.closest('.autosuggest-wrapper')) {
            document.querySelectorAll('.suggestions-container').forEach(container => {
                container.style.display = 'none';
            });
        }
    });

    // --- BPMN Modal Logic ---
    const bpmnSvgContent = `
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 400">
            <defs><marker id="arrowhead" markerWidth="10" markerHeight="7" refX="0" refY="3.5" orient="auto"><polygon points="0 0, 10 3.5, 0 7"/></marker></defs>
            <style>
                .task { fill: #fff; stroke: #333; stroke-width: 2; }
                .gateway { fill: #fff; stroke: #333; stroke-width: 2; }
                .event { fill: #fff; stroke: #333; stroke-width: 2; }
                .start-event { stroke-width: 2; }
                .end-event { stroke-width: 4; }
                .flow { fill: none; stroke: #333; stroke-width: 1.5; marker-end: url(#arrowhead); }
                .label { font-family: 'Inter', sans-serif; font-size: 14px; text-anchor: middle; }
            </style>
            <circle class="event start-event" cx="50" cy="200" r="18"/>
            <rect class="task" x="110" y="175" width="120" height="50" rx="10"/>
            <text class="label" x="170" y="205">Create Order</text>
            <path class="gateway" d="M280 200 L 305 175 L 330 200 L 305 225 Z"/>
            <text class="label" x="305" y="170" font-size="12">Approved?</text>
            <rect class="task" x="370" y="100" width="120" height="50" rx="10"/>
            <text class="label" x="430" y="130">Ship Goods</text>
            <rect class="task" x="370" y="250" width="120" height="50" rx="10"/>
            <text class="label" x="430" y="280">Reject Order</text>
            <circle class="event end-event" cx="540" y="125" r="18"/>
            <circle class="event end-event" cx="540" y="275" r="18"/>
            <path class="flow" d="M68 200 H 110"/>
            <path class="flow" d="M230 200 H 280"/>
            <path class="flow" d="M330 200 C 350 200, 350 125, 370 125"/>
            <text class="label" x="350" y="150" font-size="12">Yes</text>
            <path class="flow" d="M330 200 C 350 200, 350 275, 370 275"/>
            <text class="label" x="350" y="250" font-size="12">No</text>
            <path class="flow" d="M490 125 H 522"/>
            <path class="flow" d="M490 275 H 522"/>
        </svg>
    `;

    const modal = document.getElementById('bpmnModal');
    const bpmnBtn = document.getElementById('bpmnPreviewBtn');
    const closeModalBtn = document.querySelector('.close-modal-btn');
    const bpmnContainer = document.getElementById('bpmn-container');

    bpmnBtn.onclick = () => {
        bpmnContainer.innerHTML = bpmnSvgContent;
        modal.style.display = "block";
    }
    closeModalBtn.onclick = () => { modal.style.display = "none"; }
    window.onclick = (event) => {
        if (event.target == modal) {
            modal.style.display = "none";
        }
    }

    // --- Initial Setup & Global Event Listeners ---
    addFilterGroupBtn.addEventListener('click', addFilterGroup);
    applyFilterBtn.addEventListener('click', parseAndApplyFilters);
    container.addEventListener('click', (e) => {
        const card = e.target.closest('.variant-card');
        if (card) { showDrilldown(card.dataset.variantId); }
    });
    closePanelBtn.addEventListener('click', () => panel.classList.remove('open'));

    // --- Initial Render ---
    addFilterGroup();
    renderVariants(variantsData);
});