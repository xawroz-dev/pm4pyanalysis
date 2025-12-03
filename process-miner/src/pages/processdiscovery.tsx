// @ts-nocheck
import React, { useState, useEffect, useRef } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Progress } from '@/components/ui/progress';
import {
    Workflow,
    ZoomIn,
    ZoomOut,
    Maximize2,
    Download,
    Filter,
    RefreshCw,
    Activity,
    Clock,
    Users,
    TrendingUp,
    Upload,
    Sparkles,
    FileText,
    CheckCircle2,
    Loader2,
    Shield,
    AlertTriangle,
    Play,
    Pause,
    Wand2
} from 'lucide-react';
import { cn } from '@/lib/utils';
import { useDemo } from '@/context/DemoContext';
import BpmnViewer from 'bpmn-js';

const creditBpmnXml = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="Process_Credit" isExecutable="false">
    <bpmn:startEvent id="StartEvent_1" name="Credit Request Received">
      <bpmn:outgoing>Flow_1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:task id="Task_1" name="Initial Validation">
      <bpmn:incoming>Flow_1</bpmn:incoming>
      <bpmn:outgoing>Flow_2</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_1" sourceRef="StartEvent_1" targetRef="Task_1" />
    <bpmn:task id="Task_2" name="Credit Score Check">
      <bpmn:incoming>Flow_2</bpmn:incoming>
      <bpmn:outgoing>Flow_3</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_2" sourceRef="Task_1" targetRef="Task_2" />
    <bpmn:exclusiveGateway id="Gateway_1" name="Amount?">
      <bpmn:incoming>Flow_3</bpmn:incoming>
      <bpmn:outgoing>Flow_4</bpmn:outgoing>
      <bpmn:outgoing>Flow_5</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="Flow_3" sourceRef="Task_2" targetRef="Gateway_1" />
    <bpmn:task id="Task_3" name="Risk Assessment">
      <bpmn:incoming>Flow_4</bpmn:incoming>
      <bpmn:outgoing>Flow_6</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_4" name="> $10k" sourceRef="Gateway_1" targetRef="Task_3" />
    <bpmn:task id="Task_4" name="Manager Approval">
      <bpmn:incoming>Flow_5</bpmn:incoming>
      <bpmn:incoming>Flow_6</bpmn:incoming>
      <bpmn:outgoing>Flow_7</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_5" name="<= $10k" sourceRef="Gateway_1" targetRef="Task_4" />
    <bpmn:sequenceFlow id="Flow_6" sourceRef="Task_3" targetRef="Task_4" />
    <bpmn:exclusiveGateway id="Gateway_2" name="Approved?">
      <bpmn:incoming>Flow_7</bpmn:incoming>
      <bpmn:outgoing>Flow_8</bpmn:outgoing>
      <bpmn:outgoing>Flow_9</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="Flow_7" sourceRef="Task_4" targetRef="Gateway_2" />
    <bpmn:task id="Task_5" name="Credit Limit Updated">
      <bpmn:incoming>Flow_8</bpmn:incoming>
      <bpmn:outgoing>Flow_10</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_8" name="Yes" sourceRef="Gateway_2" targetRef="Task_5" />
    <bpmn:task id="Task_6" name="Request Denied">
      <bpmn:incoming>Flow_9</bpmn:incoming>
      <bpmn:outgoing>Flow_11</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_9" name="No" sourceRef="Gateway_2" targetRef="Task_6" />
    <bpmn:task id="Task_7" name="Customer Notified">
      <bpmn:incoming>Flow_10</bpmn:incoming>
      <bpmn:incoming>Flow_11</bpmn:incoming>
      <bpmn:outgoing>Flow_12</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_10" sourceRef="Task_5" targetRef="Task_7" />
    <bpmn:sequenceFlow id="Flow_11" sourceRef="Task_6" targetRef="Task_7" />
    <bpmn:endEvent id="EndEvent_1" name="Process End">
      <bpmn:incoming>Flow_12</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="Flow_12" sourceRef="Task_7" targetRef="EndEvent_1" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_Credit">
      <bpmndi:BPMNShape id="_BPMNShape_StartEvent_2" bpmnElement="StartEvent_1">
        <dc:Bounds x="152" y="102" width="36" height="36" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="134" y="145" width="73" height="27" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Task_1_di" bpmnElement="Task_1">
        <dc:Bounds x="240" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_1_di" bpmnElement="Flow_1">
        <di:waypoint x="188" y="120" />
        <di:waypoint x="240" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_2_di" bpmnElement="Task_2">
        <dc:Bounds x="390" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_2_di" bpmnElement="Flow_2">
        <di:waypoint x="340" y="120" />
        <di:waypoint x="390" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Gateway_1_di" bpmnElement="Gateway_1" isMarkerVisible="true">
        <dc:Bounds x="545" y="95" width="50" height="50" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="548" y="71" width="44" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_3_di" bpmnElement="Flow_3">
        <di:waypoint x="490" y="120" />
        <di:waypoint x="545" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_3_di" bpmnElement="Task_3">
        <dc:Bounds x="650" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_4_di" bpmnElement="Flow_4">
        <di:waypoint x="595" y="120" />
        <di:waypoint x="650" y="120" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="604" y="102" width="34" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_4_di" bpmnElement="Task_4">
        <dc:Bounds x="800" y="190" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_5_di" bpmnElement="Flow_5">
        <di:waypoint x="570" y="145" />
        <di:waypoint x="570" y="230" />
        <di:waypoint x="800" y="230" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="579" y="185" width="41" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_6_di" bpmnElement="Flow_6">
        <di:waypoint x="750" y="120" />
        <di:waypoint x="850" y="120" />
        <di:waypoint x="850" y="190" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Gateway_2_di" bpmnElement="Gateway_2" isMarkerVisible="true">
        <dc:Bounds x="955" y="205" width="50" height="50" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="953" y="181" width="53" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_7_di" bpmnElement="Flow_7">
        <di:waypoint x="900" y="230" />
        <di:waypoint x="955" y="230" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_5_di" bpmnElement="Task_5">
        <dc:Bounds x="1060" y="190" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_8_di" bpmnElement="Flow_8">
        <di:waypoint x="1005" y="230" />
        <di:waypoint x="1060" y="230" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="1024" y="212" width="18" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_6_di" bpmnElement="Task_6">
        <dc:Bounds x="1060" y="300" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_9_di" bpmnElement="Flow_9">
        <di:waypoint x="980" y="255" />
        <di:waypoint x="980" y="340" />
        <di:waypoint x="1060" y="340" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="988" y="295" width="15" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_7_di" bpmnElement="Task_7">
        <dc:Bounds x="1220" y="245" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_10_di" bpmnElement="Flow_10">
        <di:waypoint x="1160" y="230" />
        <di:waypoint x="1190" y="230" />
        <di:waypoint x="1190" y="285" />
        <di:waypoint x="1220" y="285" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_11_di" bpmnElement="Flow_11">
        <di:waypoint x="1160" y="340" />
        <di:waypoint x="1190" y="340" />
        <di:waypoint x="1190" y="285" />
        <di:waypoint x="1220" y="285" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="EndEvent_1_di" bpmnElement="EndEvent_1">
        <dc:Bounds x="1372" y="267" width="36" height="36" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="1359" y="310" width="63" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_12_di" bpmnElement="Flow_12">
        <di:waypoint x="1320" y="285" />
        <di:waypoint x="1372" y="285" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

const agentsBpmnXml = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" id="Definitions_2" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="Process_Agents" isExecutable="false">
    <bpmn:startEvent id="StartEvent_A1" name="Request Received">
      <bpmn:outgoing>Flow_A1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:task id="Task_A1" name="Triage Agent: Analyze">
      <bpmn:incoming>Flow_A1</bpmn:incoming>
      <bpmn:outgoing>Flow_A2</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_A1" sourceRef="StartEvent_A1" targetRef="Task_A1" />
    <bpmn:task id="Task_A2" name="Identity Agent: Verify">
      <bpmn:incoming>Flow_A2</bpmn:incoming>
      <bpmn:outgoing>Flow_A3</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_A2" sourceRef="Task_A1" targetRef="Task_A2" />
    <bpmn:exclusiveGateway id="Gateway_A1" name="Verified?">
      <bpmn:incoming>Flow_A3</bpmn:incoming>
      <bpmn:outgoing>Flow_A4</bpmn:outgoing>
      <bpmn:outgoing>Flow_A5</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="Flow_A3" sourceRef="Task_A2" targetRef="Gateway_A1" />
    <bpmn:task id="Task_A3" name="System Agent: Update">
      <bpmn:incoming>Flow_A4</bpmn:incoming>
      <bpmn:outgoing>Flow_A6</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_A4" name="Yes" sourceRef="Gateway_A1" targetRef="Task_A3" />
    <bpmn:task id="Task_A4" name="Escalate to Human">
      <bpmn:incoming>Flow_A5</bpmn:incoming>
      <bpmn:outgoing>Flow_A7</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_A5" name="No" sourceRef="Gateway_A1" targetRef="Task_A4" />
    <bpmn:task id="Task_A5" name="Card Agent: Issue">
      <bpmn:incoming>Flow_A6</bpmn:incoming>
      <bpmn:outgoing>Flow_A8</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_A6" sourceRef="Task_A3" targetRef="Task_A5" />
    <bpmn:task id="Task_A6" name="Notify Agent: Send">
      <bpmn:incoming>Flow_A8</bpmn:incoming>
      <bpmn:incoming>Flow_A7</bpmn:incoming>
      <bpmn:outgoing>Flow_A9</bpmn:outgoing>
    </bpmn:task>
    <bpmn:sequenceFlow id="Flow_A8" sourceRef="Task_A5" targetRef="Task_A6" />
    <bpmn:sequenceFlow id="Flow_A7" sourceRef="Task_A4" targetRef="Task_A6" />
    <bpmn:endEvent id="EndEvent_A1" name="Request Closed">
      <bpmn:incoming>Flow_A9</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="Flow_A9" sourceRef="Task_A6" targetRef="EndEvent_A1" />
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_2">
    <bpmndi:BPMNPlane id="BPMNPlane_2" bpmnElement="Process_Agents">
      <bpmndi:BPMNShape id="StartEvent_A1_di" bpmnElement="StartEvent_A1">
        <dc:Bounds x="152" y="102" width="36" height="36" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="126" y="145" width="89" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Task_A1_di" bpmnElement="Task_A1">
        <dc:Bounds x="240" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A1_di" bpmnElement="Flow_A1">
        <di:waypoint x="188" y="120" />
        <di:waypoint x="240" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_A2_di" bpmnElement="Task_A2">
        <dc:Bounds x="390" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A2_di" bpmnElement="Flow_A2">
        <di:waypoint x="340" y="120" />
        <di:waypoint x="390" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Gateway_A1_di" bpmnElement="Gateway_A1" isMarkerVisible="true">
        <dc:Bounds x="545" y="95" width="50" height="50" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="548" y="71" width="44" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A3_di" bpmnElement="Flow_A3">
        <di:waypoint x="490" y="120" />
        <di:waypoint x="545" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_A3_di" bpmnElement="Task_A3">
        <dc:Bounds x="650" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A4_di" bpmnElement="Flow_A4">
        <di:waypoint x="595" y="120" />
        <di:waypoint x="650" y="120" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="614" y="102" width="18" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_A4_di" bpmnElement="Task_A4">
        <dc:Bounds x="650" y="190" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A5_di" bpmnElement="Flow_A5">
        <di:waypoint x="570" y="145" />
        <di:waypoint x="570" y="230" />
        <di:waypoint x="650" y="230" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="578" y="185" width="15" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_A5_di" bpmnElement="Task_A5">
        <dc:Bounds x="800" y="80" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A6_di" bpmnElement="Flow_A6">
        <di:waypoint x="750" y="120" />
        <di:waypoint x="800" y="120" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="Task_A6_di" bpmnElement="Task_A6">
        <dc:Bounds x="950" y="135" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A8_di" bpmnElement="Flow_A8">
        <di:waypoint x="900" y="120" />
        <di:waypoint x="925" y="120" />
        <di:waypoint x="925" y="175" />
        <di:waypoint x="950" y="175" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_A7_di" bpmnElement="Flow_A7">
        <di:waypoint x="750" y="230" />
        <di:waypoint x="925" y="230" />
        <di:waypoint x="925" y="175" />
        <di:waypoint x="950" y="175" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNShape id="EndEvent_A1_di" bpmnElement="EndEvent_A1">
        <dc:Bounds x="1102" y="157" width="36" height="36" />
        <bpmndi:BPMNLabel>
          <dc:Bounds x="1080" y="200" width="80" height="14" />
        </bpmndi:BPMNLabel>
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_A9_di" bpmnElement="Flow_A9">
        <di:waypoint x="1050" y="175" />
        <di:waypoint x="1102" y="175" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>`;

export default function ProcessDiscovery() {
    const { data } = useDemo();
    const { metrics, useCase } = data;
    const containerRef = useRef(null);
    const [viewer, setViewer] = useState(null);

    const [zoom, setZoom] = useState(1);
    const [showUploadModal, setShowUploadModal] = useState(false);
    const [uploadedFile, setUploadedFile] = useState(null);
    const [analyzing, setAnalyzing] = useState(false);
    const [analysisProgress, setAnalysisProgress] = useState(0);
    const [analysisComplete, setAnalysisComplete] = useState(false);

    useEffect(() => {
        if (containerRef.current) {
            // Clear previous viewer if exists
            if (viewer) {
                viewer.destroy();
            }

            const bpmnViewer = new BpmnViewer({
                container: containerRef.current,
                height: 400
            });

            const xmlToLoad = useCase?.name?.includes('Agent') ? agentsBpmnXml : creditBpmnXml;

            bpmnViewer.importXML(xmlToLoad).then(() => {
                const canvas = bpmnViewer.get('canvas');
                canvas.zoom('fit-viewport');
                setViewer(bpmnViewer);
            }).catch(err => {
                console.error('Error rendering BPMN:', err);
            });

            return () => {
                bpmnViewer.destroy();
            };
        }
    }, [containerRef, useCase]); // Re-run when useCase changes

    const handleZoomIn = () => {
        if (viewer) {
            const canvas = viewer.get('canvas');
            canvas.zoom(canvas.zoom() * 1.1);
            setZoom(canvas.zoom());
        }
    };

    const handleZoomOut = () => {
        if (viewer) {
            const canvas = viewer.get('canvas');
            canvas.zoom(canvas.zoom() * 0.9);
            setZoom(canvas.zoom());
        }
    };

    const handleFit = () => {
        if (viewer) {
            const canvas = viewer.get('canvas');
            canvas.zoom('fit-viewport');
            setZoom(canvas.zoom());
        }
    };

    const startAnalysis = () => {
        setAnalyzing(true);
        setAnalysisProgress(0);

        const interval = setInterval(() => {
            setAnalysisProgress(prev => {
                if (prev >= 100) {
                    clearInterval(interval);
                    setAnalyzing(false);
                    setAnalysisComplete(true);
                    return 100;
                }
                return prev + 12;
            });
        }, 400);
    };

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
                <div>
                    <h1 className="text-2xl font-bold text-slate-900">Process Discovery</h1>
                    <p className="text-slate-500">Visualize and analyze your discovered processes</p>
                </div>
                <div className="flex items-center gap-2">
                    <Button variant="outline" className="gap-2" onClick={() => setShowUploadModal(true)}>
                        <Upload className="w-4 h-4" />
                        Upload Original Process
                    </Button>
                    <Button className="gap-2 bg-gradient-to-r from-blue-600 to-indigo-600">
                        <Download className="w-4 h-4" />
                        Export BPMN
                    </Button>
                </div>
            </div>

            {/* Analysis Complete Banner */}
            {analysisComplete && (
                <Card className="border-2 border-emerald-200 bg-gradient-to-r from-emerald-50 to-teal-50 shadow-lg shadow-emerald-100/50">
                    <CardContent className="p-4">
                        <div className="flex items-center gap-4">
                            <div className="p-3 rounded-xl bg-emerald-100">
                                <CheckCircle2 className="w-6 h-6 text-emerald-600" />
                            </div>
                            <div className="flex-1">
                                <h3 className="font-semibold text-emerald-900">Original Process Analyzed Successfully</h3>
                                <p className="text-sm text-emerald-700">
                                    AI has identified 5 risks and 8 controls from your documentation.
                                    <span className="font-medium"> 3 new controls have been auto-configured.</span>
                                </p>
                            </div>
                            <div className="flex items-center gap-2">
                                <Badge className="bg-red-100 text-red-700 border-0 gap-1">
                                    <AlertTriangle className="w-3 h-3" />
                                    5 Risks
                                </Badge>
                                <Badge className="bg-blue-100 text-blue-700 border-0 gap-1">
                                    <Shield className="w-3 h-3" />
                                    8 Controls
                                </Badge>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Metrics */}
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                {metrics.map((metric, index) => (
                    <Card key={index} className="border-0 shadow-lg shadow-slate-200/50">
                        <CardContent className="p-4">
                            <div className="flex items-center gap-3">
                                <div className={`p-2.5 rounded-xl ${metric.bg}`}>
                                    <metric.icon className={`w-5 h-5 ${metric.color}`} />
                                </div>
                                <div>
                                    <p className="text-2xl font-bold text-slate-900">{metric.value}</p>
                                    <p className="text-sm text-slate-500">{metric.label}</p>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>

            {/* Process Map */}
            <Card className="border-0 shadow-xl shadow-slate-200/50">
                <CardHeader className="pb-2">
                    <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-600 to-purple-600 flex items-center justify-center">
                                <Workflow className="w-5 h-5 text-white" />
                            </div>
                            <div>
                                <CardTitle>{useCase?.name || 'Process Map'}</CardTitle>
                                <CardDescription>BPMN visualization from discovered event logs</CardDescription>
                            </div>
                        </div>
                        <div className="flex items-center gap-2">
                            <Button
                                size="icon"
                                variant="outline"
                                onClick={handleZoomOut}
                            >
                                <ZoomOut className="w-4 h-4" />
                            </Button>
                            <Button
                                size="icon"
                                variant="outline"
                                onClick={handleZoomIn}
                            >
                                <ZoomIn className="w-4 h-4" />
                            </Button>
                            <Button size="icon" variant="outline" onClick={handleFit}>
                                <Maximize2 className="w-4 h-4" />
                            </Button>
                        </div>
                    </div>
                </CardHeader>
                <CardContent>
                    <div className="rounded-xl bg-gradient-to-br from-slate-50 to-slate-100 p-4 overflow-hidden h-[400px]">
                        <div ref={containerRef} className="w-full h-full" />
                    </div>
                </CardContent>
            </Card>

            {/* Upload Modal */}
            {showUploadModal && (
                <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
                    <Card className="w-full max-w-lg border-0 shadow-2xl">
                        <CardHeader>
                            <CardTitle className="flex items-center gap-3">
                                <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-600 to-purple-600 flex items-center justify-center">
                                    <FileText className="w-5 h-5 text-white" />
                                </div>
                                Upload Original Process Document
                            </CardTitle>
                            <CardDescription>
                                Upload your process documentation with risks and controls
                            </CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-4">
                            {!analyzing && !analysisComplete ? (
                                <>
                                    <label className={cn(
                                        "flex flex-col items-center justify-center h-40 rounded-xl border-2 border-dashed transition-all cursor-pointer",
                                        uploadedFile
                                            ? "border-emerald-400 bg-emerald-50"
                                            : "border-slate-300 hover:border-violet-400 hover:bg-violet-50/50"
                                    )}>
                                        <input
                                            type="file"
                                            className="hidden"
                                            accept=".pdf,.doc,.docx,.xlsx,.bpmn"
                                            onChange={(e) => setUploadedFile(e.target.files?.[0])}
                                        />
                                        {uploadedFile ? (
                                            <>
                                                <CheckCircle2 className="w-10 h-10 text-emerald-500 mb-2" />
                                                <p className="font-medium text-emerald-700">{uploadedFile.name}</p>
                                                <p className="text-sm text-emerald-600">Ready for analysis</p>
                                            </>
                                        ) : (
                                            <>
                                                <Upload className="w-10 h-10 text-slate-400 mb-2" />
                                                <p className="text-sm text-slate-600">Drop your process documentation here</p>
                                                <p className="text-xs text-slate-400">PDF, Word, Excel, or BPMN files</p>
                                            </>
                                        )}
                                    </label>

                                    <div className="p-4 rounded-xl bg-violet-50 border border-violet-200">
                                        <div className="flex items-start gap-3">
                                            <Sparkles className="w-5 h-5 text-violet-600 flex-shrink-0" />
                                            <div>
                                                <p className="font-medium text-violet-900">AI-Powered Analysis</p>
                                                <p className="text-sm text-violet-700">
                                                    AI will analyze your document to extract risks, controls, and compare with discovered process.
                                                </p>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="flex justify-end gap-2">
                                        <Button variant="outline" onClick={() => { setShowUploadModal(false); setUploadedFile(null); }}>
                                            Cancel
                                        </Button>
                                        <Button
                                            disabled={!uploadedFile}
                                            onClick={startAnalysis}
                                            className="bg-gradient-to-r from-violet-600 to-purple-600"
                                        >
                                            <Sparkles className="w-4 h-4 mr-2" />
                                            Analyze Document
                                        </Button>
                                    </div>
                                </>
                            ) : analyzing ? (
                                <div className="py-8 text-center">
                                    <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-gradient-to-br from-violet-100 to-purple-100 mb-4">
                                        <Loader2 className="w-8 h-8 text-violet-600 animate-spin" />
                                    </div>
                                    <h3 className="text-lg font-semibold text-slate-900 mb-2">Analyzing Document</h3>
                                    <p className="text-slate-500 mb-4">Extracting risks and controls...</p>
                                    <Progress value={analysisProgress} className="h-2 max-w-xs mx-auto" />
                                    <p className="text-sm text-slate-500 mt-2">{analysisProgress}% complete</p>
                                </div>
                            ) : (
                                <div className="py-8 text-center">
                                    <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-emerald-100 mb-4">
                                        <CheckCircle2 className="w-8 h-8 text-emerald-600" />
                                    </div>
                                    <h3 className="text-lg font-semibold text-slate-900 mb-2">Analysis Complete!</h3>
                                    <p className="text-slate-500 mb-4">
                                        Found 5 risks and 8 controls. Auto-configured 3 new monitoring rules.
                                    </p>
                                    <div className="flex justify-center gap-2">
                                        <Badge className="bg-red-100 text-red-700 border-0">5 Risks</Badge>
                                        <Badge className="bg-blue-100 text-blue-700 border-0">8 Controls</Badge>
                                        <Badge className="bg-emerald-100 text-emerald-700 border-0">3 Auto-configured</Badge>
                                    </div>
                                    <Button
                                        onClick={() => { setShowUploadModal(false); }}
                                        className="mt-6 bg-gradient-to-r from-emerald-600 to-teal-600"
                                    >
                                        View in Risk & Controls
                                    </Button>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </div>
            )}
        </div>
    );
}